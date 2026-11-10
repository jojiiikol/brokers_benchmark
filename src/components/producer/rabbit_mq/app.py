import asyncio
import json
import logging
import os
import time
from typing import Any

import aio_pika
from aiormq import AMQPConnectionError
from dotenv import load_dotenv
import argparse
from src.logger.config import setup_logging
load_dotenv()
logger = logging.getLogger("benchmark.producer.rabbitmq")

class Producer:
    CONNECTION_URL: str = ""
    TPS: int = 1
    MESSAGE_SIZE: int = 1024


    def __init__(
            self,
            queue_name: str = "scenario",
            broker_user: str = "guest",
            broker_password: str = "guest",
            broker_host: str = "rabbit_mq",
            broker_port: str = "5672",

            tps: int = 1,
            message_size: int = 1024,
            interaction_mode: str = "work_queue",

    ):
        if interaction_mode not in ("work_queue", "pub_sub"):
            raise ValueError(f"Unsupported interaction mode: {interaction_mode}")
        self.CONNECTION_URL = f"amqp://{broker_user}:{broker_password}@{broker_host}:{broker_port}/"

        self._connection = None
        self._channel = None
        self._exchange = None

        self.TPS = tps
        self.MESSAGE_SIZE = message_size

        self._interval = 1.0 / self.TPS

        self.queue_name = queue_name
        self.interaction_mode = interaction_mode


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

    async def declare_topology(self) -> None:
        if self._channel is None:
            raise RuntimeError("Producer is not connected")

        if self.interaction_mode == "pub_sub":
            self._exchange = await self._channel.declare_exchange(
                name=self.queue_name,
                type=aio_pika.ExchangeType.FANOUT,
                durable=True,
            )
            logger.info("Declared RabbitMQ fanout exchange %s", self.queue_name)
        else:
            await self._channel.declare_queue(
                name=self.queue_name,
                durable=True,
            )
            self._exchange = self._channel.default_exchange
            logger.info("Declared RabbitMQ work queue %s", self.queue_name)

    async def publish(
            self,
            data: dict[str, Any] | None = None,
            *,
            phase: str = "measurement",
    ) -> None:
        if self._channel is None:
            raise RuntimeError("Producer is not connected")
        if self._exchange is None:
            raise RuntimeError("Producer topology is not declared")

        if data is None:
            body = self._create_payload(phase, time.monotonic_ns())
        else:
            # Compatibility path for callers that provide their own payload.
            # The benchmark path above is the only one with an exact body size.
            data["sent_at"] = time.monotonic_ns()
            body = json.dumps(data, separators=(",", ":")).encode("utf-8")

        message = aio_pika.Message(
            body=body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        )

        await self._exchange.publish(
            message,
            routing_key="" if self.interaction_mode == "pub_sub" else self.queue_name,
        )
        logger.debug("Published RabbitMQ message for phase=%s", phase)

    async def publish_many(
            self,
            messages: list[dict[str, Any]],
    ) -> None:
        loop = asyncio.get_running_loop()
        next_publish = loop.time()

        for data in messages:
            delay = next_publish - loop.time()

            if delay > 0:
                await asyncio.sleep(delay)

            await self.publish(data)
            next_publish += self._interval


    async def publish_for(self, duration_seconds: float, phase: str) -> None:
        """Publish at the configured rate for one benchmark phase."""
        logger.info(
            "Starting RabbitMQ %s phase: duration_seconds=%s rps=%s",
            phase, duration_seconds, self.TPS,
        )
        loop = asyncio.get_running_loop()
        next_publish = loop.time()
        deadline = next_publish + duration_seconds

        while next_publish < deadline:
            delay = next_publish - loop.time()

            if delay > 0:
                await asyncio.sleep(delay)

            await self.publish(phase=phase)

            next_publish += self._interval
        logger.info("Finished RabbitMQ %s phase", phase)

    async def run_benchmark(
            self,
            warmup_seconds: float,
            duration_seconds: float,
    ) -> None:
        await self.publish_for(warmup_seconds, phase="warmup")
        await self.publish_for(duration_seconds, phase="measurement")

    def _create_payload(self, phase: str, sent_at: int) -> bytes:
        """Build a JSON message whose complete UTF-8 body has an exact size."""
        target_size = self.MESSAGE_SIZE * 1024
        payload = {
            "data": "",
            "phase": phase,
            "sent_at": sent_at,
        }
        empty_body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        data_size = target_size - len(empty_body)
        if data_size < 0:
            raise ValueError(
                f"payload_size={self.MESSAGE_SIZE} KiB is too small: "
                f"message metadata requires {len(empty_body)} bytes"
            )

        payload["data"] = "a" * data_size
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        if len(body) != target_size:
            raise RuntimeError(
                f"Expected a {target_size}-byte message, got {len(body)} bytes"
            )
        return body



    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._channel = None
            logger.info("Closed RabbitMQ producer connection")

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
        "--rps", type=int, default=1, help="Request per second for message sending"
    )
    parser.add_argument(
        "--payload_size", type=int, default=1024, help="Payload size for message sending in Kbytes"
    )
    parser.add_argument(
        "--warmup-seconds", type=float, default=0,
        help="Duration of the unmeasured message-flow warm-up phase",
    )
    parser.add_argument(
        "--duration-seconds", type=float, required=True,
        help="Duration of the measured message-flow phase",
    )
    parser.add_argument(
        "--interaction-mode", choices=("work_queue", "pub_sub"), default="work_queue",
        help="RabbitMQ delivery topology",
    )
    args = parser.parse_args()
    if args.rps <= 0:
        parser.error("--rps must be greater than zero")
    if args.payload_size <= 0:
        parser.error("--payload_size must be greater than zero")
    if args.warmup_seconds < 0:
        parser.error("--warmup-seconds must be greater than or equal to zero")
    if args.duration_seconds <= 0:
        parser.error("--duration-seconds must be greater than zero")

    producer = Producer(
        queue_name="scenario",
        broker_host=args.broker_host,
        broker_port=args.broker_port,

        message_size=args.payload_size,
        tps=int(args.rps),
        interaction_mode=args.interaction_mode,
    )
    await producer._connect()
    await producer.declare_topology()
    await producer.run_benchmark(
        warmup_seconds=args.warmup_seconds,
        duration_seconds=args.duration_seconds,
    )


if __name__ == "__main__":
    asyncio.run(main())



