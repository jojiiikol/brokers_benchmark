import argparse
import asyncio
import json
import logging
import math
import time
from typing import Any

import nats
from nats.errors import NoServersError, TimeoutError
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    DeliverPolicy,
    RetentionPolicy,
    StorageType,
    StreamConfig,
)
from nats.js.errors import APIError, NotFoundError
from src.logger.config import setup_logging


STREAM_NAME = "SCENARIO"
SUBJECT = "scenario.messages"

logger = logging.getLogger("benchmark.consumer.nats")


class LatencyMetrics:
    PERCENTILES = (50, 75, 95, 99)

    def __init__(self) -> None:
        self.values_ms: list[float] = []

    def observe(self, value: float) -> None:
        self.values_ms.append(value)

    def summary(self) -> dict[str, float | int | None]:
        result: dict[str, float | int | None] = {"count": len(self.values_ms)}
        if not self.values_ms:
            return {**result, **{f"p{p}_ms": None for p in self.PERCENTILES}}
        values = sorted(self.values_ms)
        for percentile in self.PERCENTILES:
            rank = (len(values) - 1) * percentile / 100
            lower, upper = math.floor(rank), math.ceil(rank)
            result[f"p{percentile}_ms"] = values[lower] if lower == upper else values[lower] + (values[upper] - values[lower]) * (rank - lower)
        return result


class NatsConsumer:
    def __init__(
            self,
            broker_host: str,
            broker_port: str,
            consumer_name: str,
            consumer_mode: str,
            interaction_mode: str,
            metrics_report_interval_seconds: float,
    ) -> None:
        if consumer_mode not in ("pull", "push"):
            raise ValueError(f"Unsupported consumer mode {consumer_mode}")
        self.server = f"nats://{broker_host}:{broker_port}"
        self.consumer_name = consumer_name
        self.consumer_mode = consumer_mode
        self.interaction_mode = interaction_mode
        self.metrics_report_interval_seconds = metrics_report_interval_seconds
        self.nc = None
        self.js = None
        self.metrics = LatencyMetrics()
        self.metrics_by_load_step: dict[tuple[int, int], LatencyMetrics] = {}
        self.last_report_at: float | None = None

    async def connect(self) -> None:
        for _ in range(12):
            try:
                self.nc = await nats.connect(self.server, connect_timeout=2)
                self.js = self.nc.jetstream()
                await self._ensure_stream()
                logger.info("Connected to NATS JetStream at %s", self.server)
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
            await self.js.stream_info(STREAM_NAME)

    def _consumer_config(self) -> ConsumerConfig:
        config = ConsumerConfig(
            durable_name=self.consumer_name,
            # The stream is isolated per scenario.  Reading from the beginning
            # prevents a container-start race from silently dropping the first
            # warm-up or measurement messages.
            deliver_policy=DeliverPolicy.ALL,
            # JetStream pull consumers require explicit acknowledgement.
            # Push is the at-most-once baseline and therefore uses AckNone.
            ack_policy=(
                AckPolicy.EXPLICIT
                if self.consumer_mode == "pull"
                else AckPolicy.NONE
            ),
        )
        if self.consumer_mode == "push":
            config.deliver_subject = f"scenario.delivery.{self.consumer_name}"
            if self.interaction_mode == "work_queue":
                config.deliver_group = self.consumer_name
        return config

    async def consume(self) -> None:
        self.last_report_at = asyncio.get_running_loop().time()
        logger.info(
            "Started NATS consumption: delivery_mode=%s interaction_mode=%s consumer=%s",
            self.consumer_mode, self.interaction_mode, self.consumer_name,
        )
        if self.consumer_mode == "pull":
            await self._consume_pull()
        else:
            await self._consume_push()

    async def _consume_pull(self) -> None:
        subscription = await self.js.pull_subscribe(
            SUBJECT,
            durable=self.consumer_name,
            config=self._consumer_config(),
        )
        try:
            while True:
                try:
                    messages = await subscription.fetch(batch=100, timeout=1)
                except TimeoutError:
                    continue
                for message in messages:
                    self._observe(message.data)
                    await message.ack()
        finally:
            self._log_metrics(final=True)

    async def _consume_push(self) -> None:
        completed = asyncio.Event()

        async def handler(message) -> None:
            self._observe(message.data)

        queue = self.consumer_name if self.interaction_mode == "work_queue" else ""
        await self.js.subscribe(
            SUBJECT,
            queue=queue,
            durable=self.consumer_name,
            config=self._consumer_config(),
            cb=handler,
        )
        try:
            await completed.wait()
        finally:
            self._log_metrics(final=True)

    def _observe(self, body: bytes) -> None:
        try:
            message: dict[str, Any] = json.loads(body.decode())
            phase = message.get("phase", "unknown")
            sent_at = message.get("sent_at")
            if sent_at is None:
                return
            latency = (time.monotonic_ns() - sent_at) / 1_000_000
            if phase == "measurement":
                self.metrics.observe(latency)
                self._metrics_for_load_step(message).observe(latency)
                self._report_metrics_if_due(asyncio.get_running_loop().time())
        except (UnicodeDecodeError, json.JSONDecodeError):
            logger.warning("Received malformed benchmark message")

    def _report_metrics_if_due(self, now: float) -> None:
        if self.last_report_at is not None and now - self.last_report_at >= self.metrics_report_interval_seconds:
            self._log_metrics(final=False)
            self.last_report_at = now

    def _metrics_for_load_step(self, message: dict[str, Any]) -> LatencyMetrics:
        step = int(message.get("load_step", 0))
        target_rps = int(message.get("target_rps", 0))
        return self.metrics_by_load_step.setdefault((step, target_rps), LatencyMetrics())

    def _log_metrics(self, *, final: bool) -> None:
        summary = self.metrics.summary()
        if summary["count"]:
            logger.info(
                "%s latency metrics | samples=%s | p50_ms=%.3f | p75_ms=%.3f | p95_ms=%.3f | p99_ms=%.3f",
                "Final" if final else "",
                summary["count"], summary["p50_ms"], summary["p75_ms"], summary["p95_ms"], summary["p99_ms"],
            )
            for (step, target_rps), step_metrics in sorted(self.metrics_by_load_step.items()):
                step_summary = step_metrics.summary()
                logger.info(
                    "%s latency metrics | load_step=%s | target_rps=%s | samples=%s | "
                    "p50_ms=%.3f | p75_ms=%.3f | p95_ms=%.3f | p99_ms=%.3f",
                    "Final" if final else "",
                    step,
                    target_rps,
                    step_summary["count"],
                    step_summary["p50_ms"],
                    step_summary["p75_ms"],
                    step_summary["p95_ms"],
                    step_summary["p99_ms"],
                )

    async def close(self) -> None:
        if self.nc is not None:
            await self.nc.drain()
            logger.info("Closed NATS consumer connection")


async def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker-host", required=True)
    parser.add_argument("--broker-port", default="4222")
    parser.add_argument("--consumer-name", required=True)
    parser.add_argument("--consumer-mode", choices=("pull", "push"), required=True)
    parser.add_argument("--interaction-mode", choices=("work_queue", "pub_sub"), default="work_queue")
    parser.add_argument("--metrics-report-interval-seconds", type=float, default=5)
    args = parser.parse_args()
    consumer = NatsConsumer(
        args.broker_host, args.broker_port, args.consumer_name,
        args.consumer_mode, args.interaction_mode, args.metrics_report_interval_seconds,
    )
    try:
        await consumer.connect()
        await consumer.consume()
    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
