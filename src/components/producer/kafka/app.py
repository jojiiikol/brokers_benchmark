import argparse
import asyncio
import json
import logging
import time

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from src.components.common.load_profile import (
    LoadScheduler,
    LoadStep,
    add_load_profile_arguments,
    publish_with_profile,
    scheduler_from_arguments,
)
from src.logger.config import setup_logging


TOPIC_NAME = "scenario"

logger = logging.getLogger("benchmark.producer.kafka")


class KafkaProducer:
    """Publish benchmark payloads to the scenario Kafka topic."""

    def __init__(
            self,
            broker_host: str,
            broker_port: str = "9092",
            rps: int = 1,
            payload_size: int = 1,
            interaction_mode: str = "work_queue",
            load_scheduler: LoadScheduler | None = None,
    ) -> None:
        if rps <= 0:
            raise ValueError("rps must be greater than zero")
        if payload_size <= 0:
            raise ValueError("payload_size must be greater than zero")
        if interaction_mode not in ("work_queue", "pub_sub"):
            raise ValueError(f"Unsupported interaction mode: {interaction_mode}")

        self.bootstrap_servers = f"{broker_host}:{broker_port}"
        self.rps = rps
        self.load_scheduler = load_scheduler
        self.payload_size = payload_size
        self.interaction_mode = interaction_mode
        self._producer: AIOKafkaProducer | None = None

    async def connect(self) -> None:
        if self._producer is not None:
            return

        attempts = 12
        while attempts > 0:
            producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
            try:
                await producer.start()
                self._producer = producer
                logger.info("Connected to Kafka at %s", self.bootstrap_servers)
                return
            except KafkaConnectionError:
                await producer.stop()
                attempts -= 1
                logger.warning("Kafka is not ready, retrying in 2 seconds")
                await asyncio.sleep(2)
        raise RuntimeError("Could not connect to Kafka")

    async def publish(self, phase: str, load_step: LoadStep | None = None) -> None:
        if self._producer is None:
            raise RuntimeError("Producer is not connected")
        body = self._create_payload(
            phase=phase,
            sent_at=time.monotonic_ns(),
            load_step=load_step,
        )
        await self._producer.send_and_wait(TOPIC_NAME, body)

    async def publish_for(self, duration_seconds: float, phase: str) -> None:
        if self.load_scheduler is None:
            raise RuntimeError("Load scheduler is not configured")
        logger.info(
            "Starting Kafka %s phase: duration_seconds=%s load_type=%s",
            phase,
            duration_seconds,
            self.load_scheduler.load_type.value,
        )
        await publish_with_profile(
            scheduler=self.load_scheduler,
            duration_seconds=duration_seconds,
            publish=lambda step: self.publish(phase, step),
            on_step=lambda step: logger.info(
                "Kafka %s load step=%s target_rps=%s",
                phase,
                step.index,
                step.target_rps,
            ),
        )
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
            load_step: LoadStep | None = None,
    ) -> bytes:
        """Build a JSON message whose full UTF-8 body has the requested size."""
        target_size = self.payload_size * 1024
        payload = {
            "data": "",
            "phase": phase,
            "sent_at": sent_at,
            "load_step": load_step.index if load_step else 0,
            "target_rps": load_step.target_rps if load_step else self.rps,
        }
        empty_body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        data_size = target_size - len(empty_body)
        if data_size < 0:
            raise ValueError(
                f"payload_size={self.payload_size} KiB is too small: "
                f"message metadata requires {len(empty_body)} bytes"
            )
        payload["data"] = "a" * data_size
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        if len(body) != target_size:
            raise RuntimeError(f"Expected {target_size}-byte message, got {len(body)}")
        return body

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
    args = parser.parse_args()
    if args.warmup_seconds < 0:
        parser.error("--warmup-seconds must be greater than or equal to zero")
    if args.duration_seconds <= 0:
        parser.error("--duration-seconds must be greater than zero")

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
    )
    try:
        await producer.connect()
        await producer.run_benchmark(args.warmup_seconds, args.duration_seconds)
    finally:
        await producer.close()


if __name__ == "__main__":
    asyncio.run(main())
