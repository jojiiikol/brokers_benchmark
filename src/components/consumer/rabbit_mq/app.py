import asyncio
import json
import logging
import logging.config
import time
from typing import Any

import aio_pika
from aiormq import AMQPConnectionError
from dotenv import load_dotenv
import argparse
import math
load_dotenv()

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": (
                "%(asctime)s | %(levelname)s | "
                "%(name)s:%(lineno)d | %(message)s"
            ),
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"],
    },
}

logging.config.dictConfig(LOGGING_CONFIG)

logger = logging.getLogger(__name__)


class LatencyMetrics:
    """In-memory percentile aggregation for one measurement phase.

    Percentiles use linear interpolation between adjacent sorted samples. This
    keeps p50/p75/p95/p99 stable and well-defined even for small samples.
    """

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
            metrics_report_interval_seconds: float = 5,
            interaction_mode: str = "work_queue",

    ):
        if metrics_report_interval_seconds <= 0:
            raise ValueError("metrics_report_interval_seconds must be greater than zero")
        if interaction_mode not in ("work_queue", "pub_sub"):
            raise ValueError(f"Unsupported interaction mode: {interaction_mode}")
        self.CONNECTION_URL = f"amqp://{broker_user}:{broker_password}@{broker_host}:{broker_port}/"

        self._connection = None
        self._channel = None
        self._queue = None


        self.queue_name = queue_name
        self.interaction_mode = interaction_mode
        self.metrics = LatencyMetrics()
        self.metrics_report_interval_seconds = metrics_report_interval_seconds
        self._last_metrics_report_at: float | None = None


    async def _connect(self):
        if self._connection is not None:
            return self._connection

        attempts = 5
        while attempts > 0:
            try:
                self._connection = await aio_pika.connect_robust(self.CONNECTION_URL)
                self._channel = await self._connection.channel()
                return
            except AMQPConnectionError:
                logging.warning("No connection, retrying...")
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
        else:
            self._queue = await self._channel.declare_queue(
                name=self.queue_name,
                durable=True,
            )

    def latency(self, message: dict[str, Any], received_at: int) -> float | None:
        sent_at: float | None  = message.get("sent_at", None)
        if sent_at is None:
            logger.warning(f"Received message not have sent_at field: {message}")
            return None

        return (received_at - sent_at) / 1_000_000

    async def consume(self) -> None:
        if self._channel is None or self._queue is None:
            raise RuntimeError("Consumer is not connected")


        loop = asyncio.get_running_loop()
        self._last_metrics_report_at = loop.time()
        try:
            async with self._queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        received_at = time.monotonic_ns()
                        decode_message = json.loads(message.body.decode("utf-8"))

                        latency = self.latency(decode_message, received_at)
                        phase = decode_message.get("phase", "unknown")
                        if phase == "measurement" and latency is not None:
                            self.metrics.observe(latency)
                            self._report_metrics_if_due(loop.time())

                        logger.debug(
                            "Phase=%s message=%s latency_ms=%s",
                            phase,
                            message.body[:100],
                            latency,
                        )
        finally:
            self._log_metrics(final=True)

    def _report_metrics_if_due(self, now: float) -> None:
        if self._last_metrics_report_at is None:
            self._last_metrics_report_at = now
        if now - self._last_metrics_report_at >= self.metrics_report_interval_seconds:
            self._log_metrics(final=False)
            self._last_metrics_report_at = now

    def _log_metrics(self, *, final: bool) -> None:
        summary = self.metrics.summary()
        if not summary["count"]:
            return
        prefix = "Final latency metrics" if final else "Latency metrics"
        logger.info(
            "%s | samples=%s | p50_ms=%.3f | p75_ms=%.3f | "
            "p95_ms=%.3f | p99_ms=%.3f",
            prefix,
            summary["count"],
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

    async def __aenter__(self) -> "Producer":
        await self._connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()







async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--broker-host", type=str, required=True, help="RabbitMQ broker host"
    )
    parser.add_argument(
        "--broker-port", type=str, default="5672", required=False, help="RabbitMQ broker port"
    )
    parser.add_argument(
        "--metrics-report-interval-seconds", type=float, default=5,
        help="How often to log measurement latency percentiles",
    )
    parser.add_argument(
        "--interaction-mode", choices=("work_queue", "pub_sub"), default="work_queue",
        help="RabbitMQ delivery topology",
    )
    args = parser.parse_args()

    consumer = Consumer(
        queue_name="scenario",
        broker_host=args.broker_host,
        broker_port=args.broker_port,
        metrics_report_interval_seconds=args.metrics_report_interval_seconds,
        interaction_mode=args.interaction_mode,
    )
    await consumer._connect()
    await consumer.declare_queue()
    await consumer.consume(
    )



if __name__ == "__main__":
    asyncio.run(main())



