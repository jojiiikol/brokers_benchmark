import asyncio
from src.components.common.runtime import run_component
import logging
import os
import time
from src.components.common.phase_control import synchronized_producer

import aio_pika
from aiormq import AMQPConnectionError
from dotenv import load_dotenv
import argparse
from src.components.common.load_profile import (
    LoadScheduler,
    LoadType,
    add_load_profile_arguments,
    publish_with_profile,
    scheduler_from_arguments,
)
from src.components.common.producer_metrics import ProducerMetrics, wait_for_result_collection
from src.components.common.prometheus_metrics import (
    COMPONENT_METRICS_FINAL,
    ProducerPrometheusMetrics,
    start_metrics_server,
)
from src.components.common.message_payload import build_message
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
            load_scheduler: LoadScheduler | None = None,
            guarantee_type: str = "at_most_once",
            producer_id: int = 1,
            max_in_flight: int = 100,
            metrics_report_interval_seconds: float = 5,

    ):
        if interaction_mode not in ("work_queue", "pub_sub"):
            raise ValueError(f"Unsupported interaction mode: {interaction_mode}")
        if guarantee_type not in ("at_most_once", "at_least_once"):
            raise ValueError(f"Unsupported producer guarantee: {guarantee_type}")
        if max_in_flight <= 0:
            raise ValueError("max_in_flight must be greater than zero")
        self.CONNECTION_URL = f"amqp://{broker_user}:{broker_password}@{broker_host}:{broker_port}/"

        self._connection = None
        self._channel = None
        self._exchange = None

        self.load_scheduler = load_scheduler or LoadScheduler(
            load_type=LoadType.CONSTANT,
            measurement_duration_seconds=1,
            rps=tps,
        )
        self.TPS = self.load_scheduler.step_at(0).target_rps
        self.MESSAGE_SIZE = message_size

        self.queue_name = queue_name
        self.interaction_mode = interaction_mode
        self.guarantee_type = guarantee_type
        self.producer_id = producer_id
        self._sequence = 0
        self.max_in_flight = max_in_flight
        self.metrics = ProducerMetrics(
            logger,
            metrics_report_interval_seconds,
            sink=ProducerPrometheusMetrics("rabbitmq"),
        )


    async def _connect(self):
        if self._connection is not None:
            return self._connection

        attempts = 5
        while attempts > 0:
            try:
                self._connection = await aio_pika.connect_robust(self.CONNECTION_URL)
                confirms = self.guarantee_type == "at_least_once"
                self._channel = await self._connection.channel(publisher_confirms=confirms)
                logger.info(
                    "Connected to RabbitMQ at %s with publisher_confirms=%s",
                    self.CONNECTION_URL,
                    confirms,
                )
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
            *,
            phase: str = "measurement",
    ) -> None:
        if self._channel is None:
            raise RuntimeError("Producer is not connected")
        if self._exchange is None:
            raise RuntimeError("Producer topology is not declared")

        body = self._create_payload(
            phase,
            time.monotonic_ns(),
        )

        message = aio_pika.Message(
            body=body,
            delivery_mode=(aio_pika.DeliveryMode.PERSISTENT
                           if self.guarantee_type == "at_least_once"
                           else aio_pika.DeliveryMode.NOT_PERSISTENT),
            content_type="application/octet-stream",
        )

        await self._exchange.publish(
            message,
            routing_key="" if self.interaction_mode == "pub_sub" else self.queue_name,
        )
        logger.debug("Published RabbitMQ message for phase=%s", phase)

    async def publish_for(self, duration_seconds: float, phase: str) -> None:
        """Publish at the configured rate for one benchmark phase."""
        logger.info(
            "Starting RabbitMQ %s phase: duration_seconds=%s load_type=%s",
            phase,
            duration_seconds,
            self.load_scheduler.load_type.value,
        )
        self.metrics.begin_phase(phase)
        try:
            await publish_with_profile(
                scheduler=self.load_scheduler,
                duration_seconds=duration_seconds,
                publish=lambda step: self.metrics.track(
                    self.MESSAGE_SIZE * 1024,
                    lambda: self.publish(phase=phase),
                ),
                on_step=lambda step: logger.info(
                    "RabbitMQ %s target_rps=%s",
                    phase,
                    step.target_rps,
                ),
                max_in_flight=self.max_in_flight,
                on_in_flight_changed=self.metrics.set_in_flight,
            )
        finally:
            self.metrics.log_final()
        logger.info("Finished RabbitMQ %s phase", phase)

    async def run_benchmark(
            self,
            warmup_seconds: float,
            duration_seconds: float,
    ) -> None:
        await self.publish_for(warmup_seconds, phase="warmup")
        await self.publish_for(duration_seconds, phase="measurement")

    def _create_payload(
            self,
            phase: str,
            sent_at: int,
    ) -> bytes:
        """Build a binary message whose complete body has an exact size."""
        target_size = self.MESSAGE_SIZE * 1024
        self._sequence += 1
        return build_message(
            size_bytes=target_size,
            phase=phase,
            sent_at=sent_at,
            message_id=(self.producer_id << 48) | self._sequence,
        )



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
    add_load_profile_arguments(parser)
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
    parser.add_argument(
        "--guarantee-type", choices=("at_most_once", "at_least_once"), default="at_most_once",
        help="Producer delivery guarantee",
    )
    parser.add_argument("--metrics-report-interval-seconds", type=float, default=5)
    parser.add_argument("--producer-id", type=int, default=1)
    parser.add_argument("--result-retention-seconds", type=float, default=0)
    parser.add_argument("--completion-file")
    parser.add_argument("--synchronized-producer-count", type=int)
    args = parser.parse_args()
    if args.payload_size <= 0:
        parser.error("--payload_size must be greater than zero")
    if args.warmup_seconds < 0:
        parser.error("--warmup-seconds must be greater than or equal to zero")
    if args.duration_seconds <= 0:
        parser.error("--duration-seconds must be greater than zero")
    if args.result_retention_seconds < 0:
        parser.error("--result-retention-seconds must be greater than or equal to zero")

    try:
        scheduler = scheduler_from_arguments(args, args.duration_seconds)
    except ValueError as error:
        parser.error(str(error))

    producer = Producer(
        queue_name="scenario",
        broker_host=args.broker_host,
        broker_port=args.broker_port,

        message_size=args.payload_size,
        interaction_mode=args.interaction_mode,
        load_scheduler=scheduler,
        guarantee_type=args.guarantee_type,
        producer_id=args.producer_id,
        max_in_flight=args.max_in_flight,
        metrics_report_interval_seconds=args.metrics_report_interval_seconds,
    )
    start_metrics_server()
    try:
        await producer._connect()
        await producer.declare_topology()
        await synchronized_producer(producer, args)
    finally:
        await producer.close()
    if args.synchronized_producer_count is not None:
        COMPONENT_METRICS_FINAL.set(1)
    await wait_for_result_collection(args.completion_file, args.result_retention_seconds)


if __name__ == "__main__":
    run_component(main)



