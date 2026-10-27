import asyncio
import json
import logging
import logging.config
import os
from typing import Any

import aio_pika
from aiormq import AMQPConnectionError
from dotenv import load_dotenv
import argparse
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

class Producer:
    CONNECTION_URL: str = ""
    TPS: int = 1
    MESSAGE_SIZE: int = 64


    def __init__(
            self,
            queue_name: str = "scenario",
            broker_user: str = "guest",
            broker_password: str = "guest",
            broker_host: str = "rabbitmq",
            broker_port: str = "5672",
            tps: int = 1,

    ):
        self.CONNECTION_URL = f"amqp://{broker_user}:{broker_password}@{broker_host}:{broker_port}/"

        self._connection = None
        self._channel = None

        self.TPS = tps
        self._interval = 1.0 / self.TPS

        self.queue_name = queue_name


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
            raise RuntimeError("Producer is not connected")

        await self._channel.declare_queue(
            name=self.queue_name,
            durable=True,
        )

    async def publish(self, data: dict[str, Any]) -> None:
        if self._channel is None:
            raise RuntimeError("Producer is not connected")

        message = aio_pika.Message(
            body=json.dumps(data).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        )

        await self._channel.default_exchange.publish(
            message,
            routing_key=self.queue_name,
        )
        logger.info("Message published")

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


    async def publish_forever(self, data: dict) -> None:
        loop = asyncio.get_running_loop()
        next_publish = loop.time()

        while True:
            delay = next_publish - loop.time()

            if delay > 0:
                await asyncio.sleep(delay)

            await self.publish(data)
            next_publish += self._interval


    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._channel = None

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
        "--rps", type=int, default=1, help="Request per second for message sending"
    )
    parser.add_argument(
        "--payload-size", type=int, default=64, help="Payload size for message sending in Kbytes"
    )
    args = parser.parse_args()

    producer = Producer(
        queue_name="scenario",
        broker_host=args.broker_host,
        broker_port=args.broker_port,
        tps=int(args.rps),
    )
    await producer._connect()
    await producer.declare_queue()
    await producer.publish_forever(
        data={"text": "hello world"}
    )



if __name__ == "__main__":
    asyncio.run(main())



