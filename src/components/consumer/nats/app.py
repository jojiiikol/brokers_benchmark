import argparse
import asyncio
import logging
import signal
import time
from pathlib import Path

import nats
from nats.errors import NoServersError, TimeoutError
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    DeliverPolicy,
    RetentionPolicy,
    StorageType,
    StreamConfig,
)
from nats.js.errors import APIError, NotFoundError
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
    COMPONENT_METRICS_FINAL,
    CONSUMER_RECEIVED_AT_DRAIN,
    ConsumerPrometheusMetrics,
    start_metrics_server,
)
from src.components.common.phase_control import DRAIN_COMPLETE, READY


STREAM_NAME = "SCENARIO"
SUBJECT = "scenario.messages"

logger = logging.getLogger("benchmark.consumer.nats")


class NatsConsumer:
    def __init__(
            self,
            broker_host: str,
            broker_port: str,
            consumer_name: str,
            consumer_mode: str,
            interaction_mode: str,
            guarantee_type: str = "at_most_once",
            metrics_window_size: int = DEFAULT_METRICS_WINDOW_SIZE,
            latency_sample_every: int = DEFAULT_LATENCY_SAMPLE_EVERY,
            metrics_log_every_windows: int = DEFAULT_METRICS_LOG_EVERY_WINDOWS,
    ) -> None:
        if consumer_mode not in ("pull", "push"):
            raise ValueError(f"Unsupported consumer mode {consumer_mode}")
        self.server = f"nats://{broker_host}:{broker_port}"
        self.consumer_name = consumer_name
        self.consumer_mode = consumer_mode
        self.interaction_mode = interaction_mode
        if guarantee_type not in ("at_most_once", "at_least_once"):
            raise ValueError(f"Unsupported delivery guarantee: {guarantee_type}")
        self.guarantee_type = guarantee_type
        if min(metrics_window_size, latency_sample_every, metrics_log_every_windows) <= 0:
            raise ValueError("metrics window, sampling and logging intervals must be greater than zero")
        self.metrics_window_size = metrics_window_size
        self.latency_sample_every = latency_sample_every
        self.metrics_log_every_windows = metrics_log_every_windows
        self.nc = None
        self.js = None
        self.metrics = LatencyMetrics()
        self.received_messages_total = 0
        self.measurement_messages_total = 0
        self._completed_metrics_windows = 0
        self._seen_message_ids: set[int] = set()
        self.prometheus_metrics = ConsumerPrometheusMetrics("nats")

    async def connect(self) -> None:
        for _ in range(12):
            try:
                self.nc = await nats.connect(self.server, connect_timeout=2)
                self.js = self.nc.jetstream()
                await self._ensure_stream()
                logger.info("Connected to NATS JetStream at %s", self.server)
                return
            except NoServersError:
                logger.warning("NATS is not ready, retrying in 2 seconds")
                await asyncio.sleep(2)
        raise RuntimeError("Could not connect to NATS")

    async def _ensure_stream(self) -> None:
        try:
            await self.js.stream_info(STREAM_NAME)
            logger.info("Using existing NATS stream %s", STREAM_NAME)
            return
        except NotFoundError:
            pass
        try:
            await self.js.add_stream(config=StreamConfig(
                name=STREAM_NAME,
                subjects=[SUBJECT],
                storage=(
                    StorageType.FILE
                    if self.guarantee_type == "at_least_once"
                    else StorageType.MEMORY
                ),
                retention=RetentionPolicy.INTEREST,
            ))
            logger.info("Created NATS stream %s with interest retention", STREAM_NAME)
        except APIError:
            await self.js.stream_info(STREAM_NAME)

    def _consumer_config(self) -> ConsumerConfig:
        config = ConsumerConfig(
            durable_name=self.consumer_name,
            # The stream is isolated per scenario.  Reading from the beginning
            # prevents a container-start race from silently dropping the first
            # warm-up or measurement messages.
            deliver_policy=DeliverPolicy.ALL,
            # JetStream pull consumers require explicit acknowledgement.
            ack_policy=(
                AckPolicy.EXPLICIT
                if self.consumer_mode == "pull" or self.guarantee_type == "at_least_once"
                else AckPolicy.NONE
            ),
        )
        if self.consumer_mode == "push":
            config.deliver_subject = f"scenario.delivery.{self.consumer_name}"
            if self.interaction_mode == "work_queue":
                config.deliver_group = self.consumer_name
        return config

    async def consume(
            self,
            ready_file: str | None = None,
            stop_event: asyncio.Event | None = None,
            drain_event: asyncio.Event | None = None,
            drain_complete_file: str = DRAIN_COMPLETE,
    ) -> None:
        logger.info(
            "Started NATS consumption: delivery_mode=%s interaction_mode=%s consumer=%s",
            self.consumer_mode, self.interaction_mode, self.consumer_name,
        )
        if self.consumer_mode == "pull":
            await self._consume_pull(
                ready_file, stop_event, drain_event, drain_complete_file
            )
        else:
            await self._consume_push(
                ready_file, stop_event, drain_event, drain_complete_file
            )

    async def _consume_pull(
            self,
            ready_file: str | None = None,
            stop_event: asyncio.Event | None = None,
            drain_event: asyncio.Event | None = None,
            drain_complete_file: str = DRAIN_COMPLETE,
    ) -> None:
        subscription = await self.js.pull_subscribe(
            SUBJECT,
            durable=self.consumer_name,
            config=self._consumer_config(),
        )
        if ready_file is not None:
            Path(ready_file).touch()
        drain_complete = False
        try:
            while stop_event is None or not stop_event.is_set():
                try:
                    messages = await subscription.fetch(batch=100, timeout=1)
                except TimeoutError:
                    messages = []
                for message in messages:
                    self._observe(message.data)
                    await message.ack()
                if (
                    not drain_complete
                    and drain_event is not None
                    and drain_event.is_set()
                ):
                    drain_complete = await self._mark_drain_complete_if_empty(
                        drain_complete_file
                    )
        finally:
            self._log_metrics(final=True)

    async def _consume_push(
            self,
            ready_file: str | None = None,
            stop_event: asyncio.Event | None = None,
            drain_event: asyncio.Event | None = None,
            drain_complete_file: str = DRAIN_COMPLETE,
    ) -> None:
        async def handler(message) -> None:
            self._observe(message.data)
            if self.guarantee_type == "at_least_once":
                await message.ack()

        queue = self.consumer_name if self.interaction_mode == "work_queue" else ""
        await self.js.subscribe(
            SUBJECT,
            queue=queue,
            durable=self.consumer_name,
            config=self._consumer_config(),
            cb=handler,
        )
        if ready_file is not None:
            Path(ready_file).touch()
        drain_monitor = self._start_drain_monitor(drain_event, drain_complete_file)
        try:
            await (stop_event or asyncio.Event()).wait()
        finally:
            await self._cancel_task(drain_monitor)
            self._log_metrics(final=True)

    def _start_drain_monitor(
            self,
            drain_event: asyncio.Event | None,
            drain_complete_file: str,
    ) -> asyncio.Task[None] | None:
        if drain_event is None:
            return None
        return asyncio.create_task(
            self._monitor_drain(drain_event, drain_complete_file),
            name=f"nats-drain-{self.consumer_name}",
        )

    async def _monitor_drain(
            self,
            drain_event: asyncio.Event,
            drain_complete_file: str,
    ) -> None:
        """Signal when this durable has no queued or unacknowledged messages."""
        await drain_event.wait()
        while True:
            if await self._mark_drain_complete_if_empty(drain_complete_file):
                return
            await asyncio.sleep(0.25)

    async def _mark_drain_complete_if_empty(self, drain_complete_file: str) -> bool:
        try:
            info = await self.js.consumer_info(STREAM_NAME, self.consumer_name)
        except APIError as error:
            logger.warning(
                "Could not inspect NATS consumer %s during drain: %s",
                self.consumer_name,
                error,
            )
            return False

        pending = int(getattr(info, "num_pending", 0) or 0)
        ack_pending = int(getattr(info, "num_ack_pending", 0) or 0)
        if pending != 0 or ack_pending != 0:
            return False
        Path(drain_complete_file).touch()
        logger.info("NATS consumer %s completed drain", self.consumer_name)
        return True

    @staticmethod
    async def _cancel_task(task: asyncio.Task[None] | None) -> None:
        if task is None or task.done():
            if task is not None:
                await task
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _observe(self, body: bytes) -> None:
        try:
            phase, sent_at, message_id = decode_message_metadata(body)
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
        except InvalidBenchmarkMessage as error:
            logger.warning("Received malformed benchmark message: %s", error)

    def _report_metrics_if_window_complete(self) -> None:
        if self.metrics.count >= self.metrics_window_size:
            self._completed_metrics_windows += 1
            if self._completed_metrics_windows % self.metrics_log_every_windows == 0:
                self._log_metrics(final=False)
            self.metrics = LatencyMetrics()

    def _log_metrics(self, *, final: bool) -> None:
        summary = self.metrics.summary()
        if summary["count"]:
            logger.info(
                "%s latency metrics | latency_window=%s | samples=%s | received_total=%s | "
                "measurement_received_total=%s | p50_ms=%.3f | p75_ms=%.3f | "
                "p95_ms=%.3f | p99_ms=%.3f",
                "Final" if final else "",
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
        if self.nc is not None:
            # A JetStream pull subscription can have an outstanding fetch
            # request even after its durable has no pending messages.  Client
            # drain waits for that request and prevents final metrics from
            # becoming scrapeable.  Broker backlog is already verified by the
            # synchronized drain protocol, so closing the connection is the
            # correct finalization step here.
            await self.nc.close()
            self.nc = None
            self.js = None
            logger.info("Closed NATS consumer connection")


async def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker-host", required=True)
    parser.add_argument("--broker-port", default="4222")
    parser.add_argument("--consumer-name", required=True)
    parser.add_argument("--consumer-mode", choices=("pull", "push"), required=True)
    parser.add_argument("--interaction-mode", choices=("work_queue", "pub_sub"), default="work_queue")
    parser.add_argument("--guarantee-type", choices=("at_most_once", "at_least_once"), default="at_most_once")
    add_consumer_latency_arguments(parser)
    parser.add_argument("--synchronized", action="store_true")
    args = parser.parse_args()
    consumer = NatsConsumer(
        args.broker_host, args.broker_port, args.consumer_name,
        args.consumer_mode, args.interaction_mode, args.guarantee_type, args.metrics_window_size,
        args.latency_sample_every, args.metrics_log_every_windows,
    )
    start_metrics_server()
    if not args.synchronized:
        try:
            await consumer.connect()
            await consumer.consume()
        finally:
            await consumer.close()
        return

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    drain = asyncio.Event()
    loop.add_signal_handler(signal.SIGUSR1, stop.set)

    def begin_drain() -> None:
        CONSUMER_RECEIVED_AT_DRAIN.set(consumer.measurement_messages_total)
        drain.set()

    loop.add_signal_handler(signal.SIGUSR2, begin_drain)
    try:
        await consumer.connect()
        await consumer.consume(READY, stop, drain)
        await consumer.close()
        COMPONENT_METRICS_FINAL.set(1)
        # Keep the metrics endpoint alive until scenario cleanup.
        await asyncio.Event().wait()
    finally:
        await consumer.close()
        loop.remove_signal_handler(signal.SIGUSR1)
        loop.remove_signal_handler(signal.SIGUSR2)


if __name__ == "__main__":
    asyncio.run(main())
