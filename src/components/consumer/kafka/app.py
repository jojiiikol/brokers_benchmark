import argparse
import asyncio
import json
import logging
import math
import time
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from src.logger.config import setup_logging


TOPIC_NAME = "scenario"

logger = logging.getLogger("benchmark.consumer.kafka")


class LatencyMetrics:
    PERCENTILES = (50, 75, 95, 99)

    def __init__(self) -> None:
        self._values_ms: list[float] = []

    def observe(self, latency_ms: float) -> None:
        self._values_ms.append(latency_ms)

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
            metrics_report_interval_seconds: float = 5,
    ) -> None:
        if metrics_report_interval_seconds <= 0:
            raise ValueError("metrics_report_interval_seconds must be greater than zero")
        self.bootstrap_servers = f"{broker_host}:{broker_port}"
        self.group_id = group_id
        self.metrics_report_interval_seconds = metrics_report_interval_seconds
        self.metrics = LatencyMetrics()
        self.metrics_by_load_step: dict[tuple[int, int], LatencyMetrics] = {}
        self._consumer: AIOKafkaConsumer | None = None
        self._last_metrics_report_at: float | None = None

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

        loop = asyncio.get_running_loop()
        self._last_metrics_report_at = loop.time()
        logger.info("Started Kafka consumption: group_id=%s", self.group_id)
        try:
            async for message in self._consumer:
                received_at = time.monotonic_ns()
                decoded_message = json.loads(message.value.decode("utf-8"))
                phase = decoded_message.get("phase", "unknown")
                latency = self.latency(decoded_message, received_at)
                if phase == "measurement" and latency is not None:
                    self.metrics.observe(latency)
                    self._metrics_for_load_step(decoded_message).observe(latency)
                    self._report_metrics_if_due(loop.time())
                logger.debug(
                    "Phase=%s partition=%s offset=%s latency_ms=%s",
                    phase,
                    message.partition,
                    message.offset,
                    latency,
                )
        finally:
            self._log_metrics(final=True)

    @staticmethod
    def latency(message: dict[str, Any], received_at: int) -> float | None:
        sent_at = message.get("sent_at")
        if sent_at is None:
            logger.warning("Received message without sent_at field")
            return None
        return (received_at - sent_at) / 1_000_000

    def _report_metrics_if_due(self, now: float) -> None:
        if self._last_metrics_report_at is None:
            self._last_metrics_report_at = now
        if now - self._last_metrics_report_at >= self.metrics_report_interval_seconds:
            self._log_metrics(final=False)
            self._last_metrics_report_at = now

    def _metrics_for_load_step(self, message: dict[str, Any]) -> LatencyMetrics:
        step = int(message.get("load_step", 0))
        target_rps = int(message.get("target_rps", 0))
        return self.metrics_by_load_step.setdefault((step, target_rps), LatencyMetrics())

    def _log_metrics(self, *, final: bool) -> None:
        summary = self.metrics.summary()
        if not summary["count"]:
            return
        prefix = "Final latency metrics" if final else "Latency metrics"
        logger.info(
            "%s | samples=%s | p50_ms=%.3f | p75_ms=%.3f | p95_ms=%.3f | p99_ms=%.3f",
            prefix,
            summary["count"],
            summary["p50_ms"],
            summary["p75_ms"],
            summary["p95_ms"],
            summary["p99_ms"],
        )
        for (step, target_rps), step_metrics in sorted(self.metrics_by_load_step.items()):
            step_summary = step_metrics.summary()
            logger.info(
                "%s | load_step=%s | target_rps=%s | samples=%s | "
                "p50_ms=%.3f | p75_ms=%.3f | p95_ms=%.3f | p99_ms=%.3f",
                prefix,
                step,
                target_rps,
                step_summary["count"],
                step_summary["p50_ms"],
                step_summary["p75_ms"],
                step_summary["p95_ms"],
                step_summary["p99_ms"],
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
    parser.add_argument("--metrics-report-interval-seconds", type=float, default=5)
    parser.add_argument("--interaction-mode", choices=("work_queue", "pub_sub"), default="work_queue")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        broker_host=args.broker_host,
        broker_port=args.broker_port,
        group_id=args.group_id,
        metrics_report_interval_seconds=args.metrics_report_interval_seconds,
    )
    try:
        await consumer.connect()
        await consumer.consume()
    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
