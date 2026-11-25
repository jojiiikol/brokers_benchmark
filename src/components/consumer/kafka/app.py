import argparse
import asyncio
from src.components.common.runtime import run_component
import logging
import time
from src.components.common.message_ids import MessageIdTracker
from pathlib import Path
from src.components.common.phase_control import READY, synchronized_consumer

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from src.logger.config import setup_logging
from src.components.common.message_payload import (
    InvalidBenchmarkMessage,
    decode_message_metadata,
)
from src.components.common.consumer_latency import (
    DEFAULT_LATENCY_SAMPLE_EVERY,
    DEFAULT_METRICS_LOG_EVERY_WINDOWS,
    DEFAULT_METRICS_WINDOW_SIZE,
    LatencyMetrics,
    add_consumer_latency_arguments,
)
from src.components.common.prometheus_metrics import (
    ConsumerPrometheusMetrics,
    start_metrics_server,
)


TOPIC_NAME = "scenario"

logger = logging.getLogger("benchmark.consumer.kafka")


class KafkaConsumer:
    def __init__(
            self,
            broker_host: str,
            broker_port: str,
            group_id: str,
            guarantee_type: str = "at_most_once",
            metrics_window_size: int = DEFAULT_METRICS_WINDOW_SIZE,
            latency_sample_every: int = DEFAULT_LATENCY_SAMPLE_EVERY,
            metrics_log_every_windows: int = DEFAULT_METRICS_LOG_EVERY_WINDOWS,
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
        self._seen_message_ids = MessageIdTracker()
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
                max_poll_records=256,
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
        # Starting the client alone does not guarantee a group assignment.
        while not self._consumer.assignment():
            await self._consumer.getmany(timeout_ms=100)
        for partition in self._consumer.assignment():
            await self._consumer.position(partition)
        Path(READY).touch()
        try:
            while True:
                batches = await self._consumer.getmany(timeout_ms=100, max_records=256)
                if not batches:
                    await asyncio.sleep(0)
                    continue
                offsets = {}
                for partition, messages in batches.items():
                    for message in messages:
                        self._observe(message.value)
                        offsets[partition] = message.offset + 1
                    # Allow timers and heartbeats to run even when getmany()
                    # repeatedly finds data already buffered by the client.
                    await asyncio.sleep(0)
                if offsets and self.guarantee_type == "at_least_once":
                    # Commit only offsets actually processed, never the fetch
                    # position. A crash can redeliver this batch, preserving
                    # at-least-once without one network round-trip per record.
                    await self._consumer.commit(offsets)
        finally:
            self._log_metrics(final=True)

    def _observe(self, body: bytes) -> None:
        try:
            phase, sent_at, message_id = decode_message_metadata(body)
        except InvalidBenchmarkMessage as error:
            logger.warning("Received malformed benchmark message: %s", error)
            return
        self.received_messages_total += 1
        self.prometheus_metrics.received(phase)
        if self._seen_message_ids.seen(message_id):
            self.prometheus_metrics.duplicate(phase)
        if phase == "measurement":
            self.measurement_messages_total += 1
            if self.measurement_messages_total % self.latency_sample_every == 0:
                latency_seconds = (time.monotonic_ns() - sent_at) / 1_000_000_000
                self.metrics.observe(latency_seconds * 1000)
                self.prometheus_metrics.observe_latency_seconds(latency_seconds)
                self._report_metrics_if_window_complete()

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
    add_consumer_latency_arguments(parser)
    parser.add_argument("--interaction-mode", choices=("work_queue", "pub_sub"), default="work_queue")
    parser.add_argument("--guarantee-type", choices=("at_most_once", "at_least_once"), default="at_most_once")
    parser.add_argument("--synchronized", action="store_true")
    parser.add_argument("--duration-seconds", type=float)
    parser.add_argument("--load-step-duration-seconds", type=float)
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
        if args.synchronized:
            await synchronized_consumer(consumer, args.duration_seconds, step_duration=args.load_step_duration_seconds)
        else:
            await consumer.consume()
    finally:
        await consumer.close()


if __name__ == "__main__":
    run_component(main)
