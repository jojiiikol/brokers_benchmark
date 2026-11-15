import argparse
import asyncio
import logging
import math
import time

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
from src.components.common.message_payload import (
    InvalidBenchmarkMessage,
    decode_message_fields,
)
from src.components.common.prometheus_metrics import (
    ConsumerPrometheusMetrics,
    start_metrics_server,
)


STREAM_NAME = "SCENARIO"
SUBJECT = "scenario.messages"

logger = logging.getLogger("benchmark.consumer.nats")


class LatencyMetrics:
    PERCENTILES = (50, 75, 95, 99)

    def __init__(self) -> None:
        self.values_ms: list[float] = []

    def observe(self, value: float) -> None:
        self.values_ms.append(value)

    @property
    def count(self) -> int:
        return len(self.values_ms)

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
            metrics_window_size: int = 100,
            latency_sample_every: int = 10,
            metrics_log_every_windows: int = 10,
    ) -> None:
        if consumer_mode not in ("pull", "push"):
            raise ValueError(f"Unsupported consumer mode {consumer_mode}")
        self.server = f"nats://{broker_host}:{broker_port}"
        self.consumer_name = consumer_name
        self.consumer_mode = consumer_mode
        self.interaction_mode = interaction_mode
        if min(metrics_window_size, latency_sample_every, metrics_log_every_windows) <= 0:
            raise ValueError("metrics window, sampling and logging intervals must be greater than zero")
        self.metrics_window_size = metrics_window_size
        self.latency_sample_every = latency_sample_every
        self.metrics_log_every_windows = metrics_log_every_windows
        self.nc = None
        self.js = None
        self.metrics = LatencyMetrics()
        self.received_messages_total = 0
        self.measurement_messages_total = 0
        self._completed_metrics_windows = 0
        self.prometheus_metrics = ConsumerPrometheusMetrics("nats")

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
            phase, sent_at = decode_message_fields(body)
            self.received_messages_total += 1
            self.prometheus_metrics.received(phase)
            if phase == "measurement":
                self.measurement_messages_total += 1
                if self.measurement_messages_total % self.latency_sample_every == 0:
                    latency_seconds = (time.monotonic_ns() - sent_at) / 1_000_000_000
                    self.metrics.observe(latency_seconds * 1000)
                    self.prometheus_metrics.observe_latency_seconds(latency_seconds)
                    self._report_metrics_if_window_complete()
        except InvalidBenchmarkMessage as error:
            logger.warning("Received malformed benchmark message: %s", error)

    def _report_metrics_if_window_complete(self) -> None:
        if self.metrics.count >= self.metrics_window_size:
            self._completed_metrics_windows += 1
            if self._completed_metrics_windows % self.metrics_log_every_windows == 0:
                self._log_metrics(final=False)
            self.metrics = LatencyMetrics()

    def _log_metrics(self, *, final: bool) -> None:
        summary = self.metrics.summary()
        if summary["count"]:
            logger.info(
                "%s latency metrics | latency_window=%s | samples=%s | received_total=%s | "
                "measurement_received_total=%s | p50_ms=%.3f | p75_ms=%.3f | "
                "p95_ms=%.3f | p99_ms=%.3f",
                "Final" if final else "",
                self._completed_metrics_windows,
                summary["count"],
                self.received_messages_total,
                self.measurement_messages_total,
                summary["p50_ms"],
                summary["p75_ms"],
                summary["p95_ms"],
                summary["p99_ms"],
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
    parser.add_argument("--metrics-window-size", type=int, default=100)
    parser.add_argument("--latency-sample-every", type=int, default=10)
    parser.add_argument("--metrics-log-every-windows", type=int, default=10)
    args = parser.parse_args()
    consumer = NatsConsumer(
        args.broker_host, args.broker_port, args.consumer_name,
        args.consumer_mode, args.interaction_mode, args.metrics_window_size,
        args.latency_sample_every, args.metrics_log_every_windows,
    )
    start_metrics_server()
    try:
        await consumer.connect()
        await consumer.consume()
    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
