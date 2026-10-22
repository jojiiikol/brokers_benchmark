import asyncio
import logging
import os

import aio_pika
from aiormq import AMQPConnectionError
from dotenv import load_dotenv
import argparse
load_dotenv()

BROKER_HOST = os.getenv("BROKER_HOST", 'rabbitmq-8')
BROKER_PORT = os.getenv("BROKER_PORT", "5672")
BROKER_USER = os.getenv("BROKER_USER", 'guest')
BROKER_PASSWORD = os.getenv("BROKER_PASSWORD", 'guest')
QUEUE_NAME = os.getenv("QUEUE_NAME")

RABBITMQ_URL = f"amqp://{BROKER_USER}:{BROKER_PASSWORD}@{BROKER_HOST}:{BROKER_PORT}/"

SPEED = 1
MESSAGE_SIZE = 64




logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),                       # консоль
        logging.FileHandler("app.log", mode="a"),      # файл
    ],
)


logger = logging.getLogger("consumer")

async def run():
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)

        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=10)

            queue = await channel.declare_queue(
                QUEUE_NAME,
                durable=True,
            )

            print(f"Ожидание сообщений из очереди {QUEUE_NAME}...")



            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        body = message.body.decode("utf-8")
                        print(f"Получено сообщение: {body}")
    except AMQPConnectionError:
        print("No connection...")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rps", type=int, default=1, help="Request per second for message sending"
    )
    parser.add_argument(
        "--payload-size", type=int, default=64, help="Payload size for message sending in Kbytes"
    )
    args = parser.parse_args()

    global SPEED, MESSAGE_SIZE

    SPEED = args.rps
    MESSAGE_SIZE = args.payload_size
    print(SPEED, MESSAGE_SIZE)

    await run()


if __name__ == "__main__":
    asyncio.run(main())



