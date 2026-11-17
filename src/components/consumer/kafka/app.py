import argparse
import asyncio
import logging
import math
import time

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from src.logger.config import setup_logging
from src.components.common.message_payload import (
    InvalidBenchmarkMessage,
    decode_message_metadata,
)
from src.components.common.prometheus_metrics import (
    ConsumerPrometheusMetrics,
    start_metrics_server,
)


TOPIC_NAME = "scenario"

logger = logging.getLogger("benchmark.consumer.kafka")


class LatencyMetrics:
    PERCENTILES = (50, 75, 95, 99)

    def __init__(self) -> None:
        self._values_ms: list[float] = []

    def observe(self, latency_ms: float) -> None:
        self._values_ms.append(latency_ms)

    @property
    def count(self) -> int:
        return len(self._values_ms)

    def summary(self) -> dict[str, float | int | None]:
        result: dict[str, float | int | None] = {"count": len(self._values_ms)}
        for percentile in self.PERCENTILES:
            result[f"p{percentile}_ms"] = self._percentile(percentile)
        return result

    def _percentile(self, percentile: int) -> float | None:
        if not self._values_ms:
            return None
        values = sorted(self._values_ms)
        rank = (len(values) - 1) * percentile / 100
        lower, upper = math.floor(rank), math.ceil(rank)
        if lower == upper:
            return values[lower]
        return values[lower] + (values[upper] - values[lower]) * (rank - lower)


class KafkaConsumer:
    def __init__(
            self,
            broker_host: str,
            broker_port: str,
            group_id: str,
            guarantee_type: str = "at_most_once",
            metrics_window_size: int = 100,
            latency_sample_every: int = 10,
            metrics_log_every_windows: int = 10,
    ) -> None:
        if metrics_window_size <= 0:
            raise ValueError("metrics_window_size must be greater than zero")
        if latency_sample_every <= 0 or metrics_log_every_windows <= 0:
            raise ValueError("latency sampling and logging intervals must be greater than zero")
        self.bootstrap_servers = f"{broker_host}:{broker_port}"
        self.group_id = group_id
        if guarantee_type not in ("at_most_once", "at_least_once"):
            raise ValueError(f"Unsupported delivery guarantee: {guarantee_type}")
        self.guarantee_type = guarantee_type
        self.metrics_window_size = metrics_window_size
        self.latency_sample_every = latency_sample_every
        self.metrics_log_every_windows = metrics_log_every_windows
        self.metrics = LatencyMetrics()
        self.received_messages_total = 0
        self.measurement_messages_total = 0
        self._completed_metrics_windows = 0
        self._seen_message_ids: set[int] = set()
        self.prometheus_metrics = ConsumerPrometheusMetrics("kafka")
        self._consumer: AIOKafkaConsumer | None = None

    async def connect(self) -> None:
        if self._consumer is not None:
            return

        attempts = 12
        while attempts > 0:
            consumer = AIOKafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset="latest",
                enable_auto_commit=self.guarantee_type == "at_most_once",
                max_partition_fetch_bytes=2 * 1024 * 1024,
            )
            try:
                await consumer.start()
                self._consumer = consumer
                logger.info(
                    "Connected to Kafka at %s with group_id=%s",
                    self.bootstrap_servers, self.group_id,
                )
                return
            except KafkaConnectionError:
                await consumer.stop()
                attempts -= 1
                logger.warning("Kafka is not ready, retrying in 2 seconds")
                await asyncio.sleep(2)
        raise RuntimeError("Could not connect to Kafka")

    async def consume(self) -> None:
        if self._consumer is None:
            raise RuntimeError("Consumer is not connected")

        logger.info("Started Kafka consumption: group_id=%s", self.group_id)
        try:
            async for message in self._consumer:
                try:
                    phase, sent_at, message_id = decode_message_metadata(message.value)
                except InvalidBenchmarkMessage as error:
                    logger.warning("Received malformed benchmark message: %s", error)
                    continue
                self.received_messages_total += 1
                self.prometheus_metrics.received(phase)
                if message_id in self._seen_message_ids:
                    self.prometheus_metrics.duplicate(phase)
                else:
                    self._seen_message_ids.add(message_id)
                if phase == "measurement":
                    self.measurement_messages_total += 1
                    if self.measurement_messages_total % self.latency_sample_every == 0:
                        latency_seconds = (time.monotonic_ns() - sent_at) / 1_000_000_000
                        self.metrics.observe(latency_seconds * 1000)
                        self.prometheus_metrics.observe_latency_seconds(latency_seconds)
                        self._report_metrics_if_window_complete()
                if self.guarantee_type == "at_least_once":
                    await self._consumer.commit()
        finally:
            self._log_metrics(final=True)

    def _report_metrics_if_window_complete(self) -> None:
        if self.metrics.count >= self.metrics_window_size:
            self._completed_metrics_windows += 1
            if self._completed_metrics_windows % self.metrics_log_every_windows == 0:
                self._log_metrics(final=False)
            self.metrics = LatencyMetrics()

    def _log_metrics(self, *, final: bool) -> None:
        summary = self.metrics.summary()
        if not summary["count"]:
            return
        prefix = "Final latency metrics" if final else "Latency metrics"
        logger.info(
            "%s | latency_window=%s | samples=%s | received_total=%s | measurement_received_total=%s | "
            "p50_ms=%.3f | p75_ms=%.3f | p95_ms=%.3f | p99_ms=%.3f",
            prefix,
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
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
            logger.info("Closed Kafka consumer connection")


async def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker-host", required=True, help="Kafka broker host")
    parser.add_argument("--broker-port", default="9092", help="Kafka broker port")
    parser.add_argument("--group-id", required=True, help="Kafka consumer group")
    parser.add_argument("--metrics-window-size", type=int, default=100)
    parser.add_argument("--latency-sample-every", type=int, default=10)
    parser.add_argument("--metrics-log-every-windows", type=int, default=10)
    parser.add_argument("--interaction-mode", choices=("work_queue", "pub_sub"), default="work_queue")
    parser.add_argument("--guarantee-type", choices=("at_most_once", "at_least_once"), default="at_most_once")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        broker_host=args.broker_host,
        broker_port=args.broker_port,
        group_id=args.group_id,
        guarantee_type=args.guarantee_type,
        metrics_window_size=args.metrics_window_size,
        latency_sample_every=args.latency_sample_every,
        metrics_log_every_windows=args.metrics_log_every_windows,
    )
    start_metrics_server()
    try:
        await consumer.connect()
        await consumer.consume()
    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
