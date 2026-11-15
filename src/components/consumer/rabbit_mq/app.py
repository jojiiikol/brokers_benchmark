import asyncio
import logging
import time

import aio_pika
from aiormq import AMQPConnectionError
from dotenv import load_dotenv
import argparse
import math
from src.logger.config import setup_logging
from src.components.common.message_payload import (
    InvalidBenchmarkMessage,
    decode_message_fields,
)
from src.components.common.prometheus_metrics import (
    ConsumerPrometheusMetrics,
    start_metrics_server,
)
load_dotenv()
logger = logging.getLogger("benchmark.consumer.rabbitmq")


class LatencyMetrics:
    """In-memory percentile aggregation for one measurement phase."""

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
        lower = math.floor(rank)
        upper = math.ceil(rank)
        if lower == upper:
            return values[lower]
        return values[lower] + (values[upper] - values[lower]) * (rank - lower)


class Consumer:
    CONNECTION_URL: str = ""


    def __init__(
            self,
            queue_name: str = "scenario",
            broker_user: str = "guest",
            broker_password: str = "guest",
            broker_host: str = "rabbit_mq",
            broker_port: str = "5672",
            metrics_window_size: int = 100,
            latency_sample_every: int = 10,
            metrics_log_every_windows: int = 10,
            interaction_mode: str = "work_queue",
            guarantee_type: str = "at_most_once",

    ):
        if metrics_window_size <= 0:
            raise ValueError("metrics_window_size must be greater than zero")
        if latency_sample_every <= 0:
            raise ValueError("latency_sample_every must be greater than zero")
        if metrics_log_every_windows <= 0:
            raise ValueError("metrics_log_every_windows must be greater than zero")
        if interaction_mode not in ("work_queue", "pub_sub"):
            raise ValueError(f"Unsupported interaction mode: {interaction_mode}")
        if guarantee_type not in ("at_most_once", "at_least_once", "exactly_once"):
            raise ValueError(f"Unsupported delivery guarantee: {guarantee_type}")
        self.CONNECTION_URL = f"amqp://{broker_user}:{broker_password}@{broker_host}:{broker_port}/"

        self._connection = None
        self._channel = None
        self._queue = None


        self.queue_name = queue_name
        self.interaction_mode = interaction_mode
        self.guarantee_type = guarantee_type
        self.metrics = LatencyMetrics()
        self.metrics_window_size = metrics_window_size
        self.latency_sample_every = latency_sample_every
        self.metrics_log_every_windows = metrics_log_every_windows
        self.received_messages_total = 0
        self.measurement_messages_total = 0
        self._completed_metrics_windows = 0
        self.prometheus_metrics = ConsumerPrometheusMetrics("rabbitmq")

    @property
    def uses_acknowledgements(self) -> bool:
        return self.guarantee_type != "at_most_once"

    async def _connect(self):
        if self._connection is not None:
            return self._connection

        attempts = 5
        while attempts > 0:
            try:
                self._connection = await aio_pika.connect_robust(self.CONNECTION_URL)
                self._channel = await self._connection.channel()
                logger.info("Connected to RabbitMQ at %s", self.CONNECTION_URL)
                return
            except AMQPConnectionError:
                logger.warning("RabbitMQ is not ready, retrying in 5 seconds")
                await asyncio.sleep(5)
            attempts -= 1
        raise Exception("Connection failed")

    async def declare_queue(self) -> None:
        if self._channel is None:
            raise RuntimeError("Consumer is not connected")

        if self.interaction_mode == "pub_sub":
            exchange = await self._channel.declare_exchange(
                name=self.queue_name,
                type=aio_pika.ExchangeType.FANOUT,
                durable=True,
            )
            self._queue = await self._channel.declare_queue(
                name="",
                exclusive=True,
                auto_delete=True,
            )
            await self._queue.bind(exchange)
            logger.info("Bound exclusive RabbitMQ subscriber queue to %s", self.queue_name)
        else:
            self._queue = await self._channel.declare_queue(
                name=self.queue_name,
                durable=True,
            )
            logger.info("Declared RabbitMQ work queue %s", self.queue_name)

    async def consume(self) -> None:
        if self._channel is None or self._queue is None:
            raise RuntimeError("Consumer is not connected")


        no_ack = not self.uses_acknowledgements
        logger.info(
            "Started RabbitMQ consumption: mode=%s no_ack=%s",
            self.interaction_mode,
            no_ack,
        )
        try:
            async with self._queue.iterator(no_ack=no_ack) as queue_iter:
                async for message in queue_iter:
                    if no_ack:
                        self._observe_message(message.body)
                    else:
                        async with message.process():
                            self._observe_message(message.body)
        finally:
            self._log_metrics(final=True)

    def _observe_message(self, body: bytes) -> None:
        try:
            phase, sent_at = decode_message_fields(body)
        except InvalidBenchmarkMessage as error:
            logger.warning("Received malformed benchmark message: %s", error)
            return

        self.received_messages_total += 1
        self.prometheus_metrics.received(phase)
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
            "p50_ms=%.3f | p75_ms=%.3f | "
            "p95_ms=%.3f | p99_ms=%.3f",
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
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._channel = None
            self._queue = None
            logger.info("Closed RabbitMQ consumer connection")

    async def __aenter__(self) -> "Producer":
        await self._connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()







async def main():
    setup_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--broker-host", type=str, required=True, help="RabbitMQ broker host"
    )
    parser.add_argument(
        "--broker-port", type=str, default="5672", required=False, help="RabbitMQ broker port"
    )
    parser.add_argument(
        "--metrics-window-size", type=int, default=1000,
        help="Number of latency samples in one percentile window",
    )
    parser.add_argument("--latency-sample-every", type=int, default=10)
    parser.add_argument("--metrics-log-every-windows", type=int, default=100)
    parser.add_argument(
        "--interaction-mode", choices=("work_queue", "pub_sub"), default="work_queue",
        help="RabbitMQ delivery topology",
    )
    parser.add_argument(
        "--guarantee-type",
        choices=("at_most_once", "at_least_once", "exactly_once"),
        default="at_most_once",
    )
    args = parser.parse_args()

    consumer = Consumer(
        queue_name="scenario",
        broker_host=args.broker_host,
        broker_port=args.broker_port,
        metrics_window_size=args.metrics_window_size,
        latency_sample_every=args.latency_sample_every,
        metrics_log_every_windows=args.metrics_log_every_windows,
        interaction_mode=args.interaction_mode,
        guarantee_type=args.guarantee_type,
    )
    start_metrics_server()
    await consumer._connect()
    await consumer.declare_queue()
    await consumer.consume(
    )



if __name__ == "__main__":
    asyncio.run(main())



