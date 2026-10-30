import asyncio
import json
import logging
import logging.config
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

class Consumer:
    CONNECTION_URL: str = ""


    def __init__(
            self,
            queue_name: str = "scenario",
            broker_user: str = "guest",
            broker_password: str = "guest",
            broker_host: str = "rabbit_mq",
            broker_port: str = "5672",

    ):
        self.CONNECTION_URL = f"amqp://{broker_user}:{broker_password}@{broker_host}:{broker_port}/"

        self._connection = None
        self._channel = None
        self._queue = None


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
            raise RuntimeError("Consumer is not connected")

        self._queue = await self._channel.declare_queue(
            name=self.queue_name,
            durable=True,
        )

    async def consume(self) -> None:
        if self._channel is None or self._queue is None:
            raise RuntimeError("Consumer is not connected")


        async with self._queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    logging.info(f"Message: {message.body}")

                    if self._queue.name in message.body.decode():
                        break


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
    args = parser.parse_args()

    consumer = Consumer(
        queue_name="scenario",
        broker_host=args.broker_host,
        broker_port=args.broker_port,
    )
    await consumer._connect()
    await consumer.declare_queue()
    await consumer.consume(
    )



if __name__ == "__main__":
    asyncio.run(main())



