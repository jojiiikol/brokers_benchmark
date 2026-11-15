import argparse
import asyncio
import logging
import time

import nats
from nats.errors import NoServersError
from nats.js.api import RetentionPolicy, StorageType, StreamConfig
from nats.js.errors import APIError, NotFoundError
from src.components.common.load_profile import (
    LoadScheduler,
    add_load_profile_arguments,
    publish_with_profile,
    scheduler_from_arguments,
)
from src.components.common.producer_metrics import ProducerMetrics
from src.components.common.prometheus_metrics import (
    ProducerPrometheusMetrics,
    start_metrics_server,
)
from src.components.common.message_payload import build_message
from src.logger.config import setup_logging


STREAM_NAME = "SCENARIO"
SUBJECT = "scenario.messages"

logger = logging.getLogger("benchmark.producer.nats")


class NatsProducer:
    def __init__(
            self,
            broker_host: str,
            broker_port: str,
            rps: int,
            payload_size: int,
            load_scheduler: LoadScheduler,
            guarantee_type: str = "at_most_once",
            max_in_flight: int = 100,
            metrics_report_interval_seconds: float = 5,
    ) -> None:
        if rps <= 0 or payload_size <= 0:
            raise ValueError("rps and payload_size must be greater than zero")
        if guarantee_type != "at_most_once":
            raise ValueError(f"Unsupported producer guarantee: {guarantee_type}")
        if max_in_flight <= 0:
            raise ValueError("max_in_flight must be greater than zero")
        self.server = f"nats://{broker_host}:{broker_port}"
        self.rps = rps
        self.payload_size = payload_size
        self.load_scheduler = load_scheduler
        self.guarantee_type = guarantee_type
        self.max_in_flight = max_in_flight
        self.metrics = ProducerMetrics(
            logger,
            metrics_report_interval_seconds,
            sink=ProducerPrometheusMetrics("nats"),
        )
        self.nc = None
        self.js = None

    async def connect(self) -> None:
        for _ in range(12):
            try:
                self.nc = await nats.connect(self.server, connect_timeout=2)
                self.js = self.nc.jetstream()
                await self._ensure_stream()
                logger.info(
                    "Connected to NATS JetStream at %s; publishes use Core NATS without PubAck",
                    self.server,
                )
                return
            except NoServersError:
                logger.warning("NATS is not ready, retrying in 2 seconds")
                await asyncio.sleep(2)
        raise RuntimeError("Could not connect to NATS")

    async def _ensure_stream(self) -> None:
        try:
            await self.js.stream_info(STREAM_NAME)
            logger.info("Using existing NATS stream %s", STREAM_NAME)
            return
        except NotFoundError:
            pass
        try:
            await self.js.add_stream(config=StreamConfig(
                name=STREAM_NAME,
                subjects=[SUBJECT],
                storage=StorageType.MEMORY,
                retention=RetentionPolicy.INTEREST,
            ))
            logger.info("Created NATS stream %s with interest retention", STREAM_NAME)
        except APIError:
            # Concurrent producers can race to create the same stream.
            await self.js.stream_info(STREAM_NAME)

    async def publish(self, phase: str) -> None:
        body = self._create_payload(phase, time.monotonic_ns())
        # The stream still receives this subject, but Core NATS publish does
        # not wait for JetStream's PubAck. This is producer-side at-most-once.
        await self.nc.publish(SUBJECT, body)

    async def publish_for(self, duration_seconds: float, phase: str) -> None:
        logger.info(
            "Starting NATS %s phase: duration_seconds=%s load_type=%s",
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
                    "NATS %s target_rps=%s",
                    phase,
                    step.target_rps,
                ),
                max_in_flight=self.max_in_flight,
                on_in_flight_changed=self.metrics.set_in_flight,
            )
        finally:
            self.metrics.log_final()
        logger.info("Finished NATS %s phase", phase)

    async def run_benchmark(self, warmup_seconds: float, duration_seconds: float) -> None:
        await self.publish_for(warmup_seconds, "warmup")
        await self.publish_for(duration_seconds, "measurement")

    def _create_payload(self, phase: str, sent_at: int) -> bytes:
        target_size = self.payload_size * 1024
        return build_message(
            size_bytes=target_size,
            phase=phase,
            sent_at=sent_at,
        )

    async def close(self) -> None:
        if self.nc is not None:
            await self.nc.drain()
            logger.info("Closed NATS producer connection")


async def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker-host", required=True)
    parser.add_argument("--broker-port", default="4222")
    add_load_profile_arguments(parser)
    parser.add_argument("--payload_size", type=int, required=True)
    parser.add_argument("--warmup-seconds", type=float, default=0)
    parser.add_argument("--duration-seconds", type=float, required=True)
    parser.add_argument("--interaction-mode", choices=("work_queue", "pub_sub"), default="work_queue")
    parser.add_argument("--guarantee-type", choices=("at_most_once",), default="at_most_once")
    parser.add_argument("--metrics-report-interval-seconds", type=float, default=5)
    args = parser.parse_args()
    if args.warmup_seconds < 0 or args.duration_seconds <= 0:
        parser.error("Invalid benchmark durations")
    try:
        scheduler = scheduler_from_arguments(args, args.duration_seconds)
    except ValueError as error:
        parser.error(str(error))
    producer = NatsProducer(
        args.broker_host,
        args.broker_port,
        scheduler.step_at(0).target_rps,
        args.payload_size,
        scheduler,
        args.guarantee_type,
        args.max_in_flight,
        args.metrics_report_interval_seconds,
    )
    start_metrics_server()
    try:
        await producer.connect()
        await producer.run_benchmark(args.warmup_seconds, args.duration_seconds)
    finally:
        await producer.close()


if __name__ == "__main__":
    asyncio.run(main())
