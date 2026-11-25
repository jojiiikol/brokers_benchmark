import argparse
import asyncio
import logging
import time
from pathlib import Path

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from src.components.common.load_profile import (
    LoadType,
    LoadScheduler,
    add_load_profile_arguments,
    publish_with_profile,
    scheduler_from_arguments,
)
from src.components.common.phase_control import (
    MEASUREMENT_START,
    READY,
    WARMUP_DONE,
    WARMUP_START,
    wait_for_release,
)
from src.components.common.producer_metrics import ProducerMetrics, wait_for_result_collection
from src.components.common.prometheus_metrics import (
    COMPONENT_METRICS_FINAL,
    ProducerPrometheusMetrics,
    start_metrics_server,
)
from src.components.common.message_payload import build_message
from src.logger.config import setup_logging


TOPIC_NAME = "scenario"

logger = logging.getLogger("benchmark.producer.kafka")


async def run_benchmark_lifecycle(
        producer: "KafkaProducer",
        scheduler: LoadScheduler,
        *,
        warmup_seconds: float,
        duration_seconds: float,
        synchronized_producer_count: int | None,
        producer_id: int,
        ready_file: str = READY,
        warmup_start_file: str = WARMUP_START,
        warmup_done_file: str = WARMUP_DONE,
        measurement_start_file: str = MEASUREMENT_START,
) -> None:
    """Run Kafka phases either immediately or under orchestrator control."""
    if synchronized_producer_count is None:
        await producer.run_benchmark(warmup_seconds, duration_seconds)
        return
    if synchronized_producer_count <= 0:
        raise ValueError("synchronized_producer_count must be greater than zero")

    Path(ready_file).touch()
    await wait_for_release(warmup_start_file)
    await producer.publish_for(warmup_seconds, phase="warmup")
    Path(warmup_done_file).touch()

    target_rps = scheduler.step_at(0).target_rps
    offset = (
        (producer_id - 1) / (synchronized_producer_count * target_rps)
        if scheduler.load_type is LoadType.CONSTANT and target_rps
        else 0.0
    )
    await wait_for_release(measurement_start_file, offset)
    await producer.publish_for(duration_seconds - offset, phase="measurement")


class KafkaProducer:
    """Publish benchmark payloads to the scenario Kafka topic."""

    def __init__(
            self,
            broker_host: str,
            broker_port: str = "9092",
            rps: int | None = 1,
            payload_size: int = 1,
            interaction_mode: str = "work_queue",
            load_scheduler: LoadScheduler | None = None,
            guarantee_type: str = "at_most_once",
            producer_id: int = 1,
            max_in_flight: int = 100,
            metrics_report_interval_seconds: float = 5,
    ) -> None:
        if rps is not None and rps <= 0:
            raise ValueError("rps must be greater than zero")
        if payload_size <= 0:
            raise ValueError("payload_size must be greater than zero")
        if interaction_mode not in ("work_queue", "pub_sub"):
            raise ValueError(f"Unsupported interaction mode: {interaction_mode}")
        if guarantee_type not in ("at_most_once", "at_least_once"):
            raise ValueError(f"Unsupported producer guarantee: {guarantee_type}")
        if max_in_flight <= 0:
            raise ValueError("max_in_flight must be greater than zero")

        self.bootstrap_servers = f"{broker_host}:{broker_port}"
        self.rps = rps
        self.load_scheduler = load_scheduler
        self.payload_size = payload_size
        self.interaction_mode = interaction_mode
        self.guarantee_type = guarantee_type
        self.producer_id = producer_id
        self._sequence = 0
        self.max_in_flight = max_in_flight
        self.metrics = ProducerMetrics(
            logger,
            metrics_report_interval_seconds,
            sink=ProducerPrometheusMetrics("kafka"),
        )
        self._producer: AIOKafkaProducer | None = None

    async def connect(self) -> None:
        if self._producer is not None:
            return

        attempts = 12
        while attempts > 0:
            reliable = self.guarantee_type == "at_least_once"
            producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                acks="all" if reliable else 0,
                enable_idempotence=reliable,
                max_request_size=2 * 1024 * 1024,
            )
            try:
                await producer.start()
                self._producer = producer
                logger.info(
                    "Connected to Kafka at %s with acks=%s",
                    self.bootstrap_servers,
                    "all" if reliable else "0",
                )
                return
            except KafkaConnectionError:
                await producer.stop()
                attempts -= 1
                logger.warning("Kafka is not ready, retrying in 2 seconds")
                await asyncio.sleep(2)
        raise RuntimeError("Could not connect to Kafka")

    async def publish(self, phase: str) -> None:
        if self._producer is None:
            raise RuntimeError("Producer is not connected")
        body = self._create_payload(
            phase=phase,
            sent_at=time.monotonic_ns(),
        )
        if self.guarantee_type == "at_least_once":
            await self._producer.send_and_wait(TOPIC_NAME, body)
        else:
            await self._producer.send(TOPIC_NAME, body)

    async def publish_for(self, duration_seconds: float, phase: str) -> None:
        if self.load_scheduler is None:
            raise RuntimeError("Load scheduler is not configured")
        logger.info(
            "Starting Kafka %s phase: duration_seconds=%s load_type=%s",
            phase,
            duration_seconds,
            self.load_scheduler.load_type.value,
        )
        self.metrics.begin_phase(phase)
        try:
            await publish_with_profile(
                scheduler=self.load_scheduler,
                duration_seconds=duration_seconds,
                publish=lambda step: self.metrics.track(
                    self.payload_size * 1024,
                    lambda: self.publish(phase),
                ),
                on_step=lambda step: logger.info(
                    "Kafka %s target_rps=%s",
                    phase,
                    step.target_rps,
                ),
                max_in_flight=self.max_in_flight,
                on_in_flight_changed=self.metrics.set_in_flight,
            )
        finally:
            self.metrics.log_final()
        logger.info("Finished Kafka %s phase", phase)

    async def run_benchmark(
            self,
            warmup_seconds: float,
            duration_seconds: float,
    ) -> None:
        await self.publish_for(warmup_seconds, phase="warmup")
        await self.publish_for(duration_seconds, phase="measurement")

    def _create_payload(
            self,
            phase: str,
            sent_at: int,
    ) -> bytes:
        """Build a binary message whose full body has the requested size."""
        target_size = self.payload_size * 1024
        self._sequence += 1
        return build_message(
            size_bytes=target_size,
            phase=phase,
            sent_at=sent_at,
            message_id=(self.producer_id << 48) | self._sequence,
        )

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
            logger.info("Closed Kafka producer connection")


async def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker-host", required=True, help="Kafka broker host")
    parser.add_argument("--broker-port", default="9092", help="Kafka broker port")
    add_load_profile_arguments(parser)
    parser.add_argument("--payload_size", type=int, default=1, help="Message size in KiB")
    parser.add_argument("--warmup-seconds", type=float, default=0)
    parser.add_argument("--duration-seconds", type=float, required=True)
    parser.add_argument("--interaction-mode", choices=("work_queue", "pub_sub"), default="work_queue")
    parser.add_argument("--guarantee-type", choices=("at_most_once", "at_least_once"), default="at_most_once")
    parser.add_argument("--metrics-report-interval-seconds", type=float, default=5)
    parser.add_argument("--producer-id", type=int, default=1)
    parser.add_argument("--result-retention-seconds", type=float, default=0)
    parser.add_argument("--completion-file")
    parser.add_argument("--synchronized-producer-count", type=int)
    args = parser.parse_args()
    if args.warmup_seconds < 0:
        parser.error("--warmup-seconds must be greater than or equal to zero")
    if args.duration_seconds <= 0:
        parser.error("--duration-seconds must be greater than zero")
    if args.result_retention_seconds < 0:
        parser.error("--result-retention-seconds must be greater than or equal to zero")
    if (
        args.synchronized_producer_count is not None
        and args.synchronized_producer_count <= 0
    ):
        parser.error("--synchronized-producer-count must be greater than zero")

    try:
        scheduler = scheduler_from_arguments(args, args.duration_seconds)
    except ValueError as error:
        parser.error(str(error))

    producer = KafkaProducer(
        broker_host=args.broker_host,
        broker_port=args.broker_port,
        rps=scheduler.step_at(0).target_rps,
        payload_size=args.payload_size,
        interaction_mode=args.interaction_mode,
        load_scheduler=scheduler,
        guarantee_type=args.guarantee_type,
        producer_id=args.producer_id,
        max_in_flight=args.max_in_flight,
        metrics_report_interval_seconds=args.metrics_report_interval_seconds,
    )
    start_metrics_server()
    try:
        await producer.connect()
        await run_benchmark_lifecycle(
            producer,
            scheduler,
            warmup_seconds=args.warmup_seconds,
            duration_seconds=args.duration_seconds,
            synchronized_producer_count=args.synchronized_producer_count,
            producer_id=args.producer_id,
        )
    finally:
        await producer.close()
    if args.synchronized_producer_count is not None:
        COMPONENT_METRICS_FINAL.set(1)
    await wait_for_result_collection(args.completion_file, args.result_retention_seconds)


if __name__ == "__main__":
    asyncio.run(main())
