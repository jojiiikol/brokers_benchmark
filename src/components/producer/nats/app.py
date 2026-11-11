import argparse
import asyncio
import json
import logging
import time

import nats
from nats.errors import NoServersError
from nats.js.api import RetentionPolicy, StorageType, StreamConfig
from nats.js.errors import APIError, NotFoundError
from src.components.common.load_profile import (
    LoadScheduler,
    LoadStep,
    add_load_profile_arguments,
    publish_with_profile,
    scheduler_from_arguments,
)
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

    async def publish(self, phase: str, load_step: LoadStep) -> None:
        body = self._create_payload(phase, time.monotonic_ns(), load_step)
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
        await publish_with_profile(
            scheduler=self.load_scheduler,
            duration_seconds=duration_seconds,
            publish=lambda step: self.publish(phase, step),
            on_step=lambda step: logger.info(
                "NATS %s load step=%s target_rps=%s",
                phase,
                step.index,
                step.target_rps,
            ),
            max_in_flight=self.max_in_flight,
        )
        logger.info("Finished NATS %s phase", phase)

    async def run_benchmark(self, warmup_seconds: float, duration_seconds: float) -> None:
        await self.publish_for(warmup_seconds, "warmup")
        await self.publish_for(duration_seconds, "measurement")

    def _create_payload(self, phase: str, sent_at: int, load_step: LoadStep) -> bytes:
        target_size = self.payload_size * 1024
        payload = {
            "data": "",
            "phase": phase,
            "sent_at": sent_at,
            "load_step": load_step.index,
            "target_rps": load_step.target_rps,
        }
        padding = target_size - len(json.dumps(payload, separators=(",", ":")).encode())
        if padding < 0:
            raise ValueError("Message metadata is larger than the requested payload size")
        payload["data"] = "a" * padding
        body = json.dumps(payload, separators=(",", ":")).encode()
        if len(body) != target_size:
            raise RuntimeError("Unexpected message size")
        return body

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
    )
    try:
        await producer.connect()
        await producer.run_benchmark(args.warmup_seconds, args.duration_seconds)
    finally:
        await producer.close()


if __name__ == "__main__":
    asyncio.run(main())
