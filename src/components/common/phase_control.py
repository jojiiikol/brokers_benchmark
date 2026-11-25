"""Signals shared by the orchestrator and synchronized benchmark clients."""

import asyncio
from pathlib import Path
import time
import signal

READY = "/tmp/benchmark-ready"
WARMUP_START = "/tmp/benchmark-warmup-start"
WARMUP_DONE = "/tmp/benchmark-warmup-done"
MEASUREMENT_START = "/tmp/benchmark-measurement-start"


async def synchronized_producer(producer, args):
    """Phase barriers are outside the per-message path for every adapter."""
    if args.synchronized_producer_count is None:
        await producer.run_benchmark(args.warmup_seconds, args.duration_seconds)
        return
    Path(READY).touch()
    await wait_for_release(WARMUP_START)
    await producer.publish_for(args.warmup_seconds, phase="warmup")
    from src.components.common.prometheus_metrics import WARMUP_ACCEPTED, WARMUP_FINISHED
    WARMUP_ACCEPTED.set(producer.metrics.measurement_snapshot()[0])
    WARMUP_FINISHED.set(1)
    Path(WARMUP_DONE).touch()
    await wait_for_release(MEASUREMENT_START)
    snapshot = asyncio.create_task(measurement_snapshot(
        args.duration_seconds, producer.metrics.measurement_snapshot,
        step_duration=getattr(args, "step_duration_seconds", None)))
    try:
        await producer.publish_for(args.duration_seconds, phase="measurement")
        await snapshot
    finally:
        snapshot.cancel()
        await asyncio.gather(snapshot, return_exceptions=True)


async def measurement_snapshot(duration, counters, *, step_duration=None, latency=None):
    from src.components.common.prometheus_metrics import (
        MEASUREMENT_END_MESSAGES, MEASUREMENT_END_BYTES, MEASUREMENT_SNAPSHOT_LATENESS,
        MEASUREMENT_SNAPSHOT_COMPLETE,
        STEP_MESSAGES, STEP_BYTES, STEP_LATENESS,
        STEP_LATENCY_BUCKET, STEP_LATENCY_COUNT, STEP_LATENCY_SUM,
    )
    count = round(duration/step_duration) if step_duration else 1
    previous_messages, previous_bytes, previous_latency = 0, 0, {}
    for index in range(count):
        offset = (index+1)*step_duration if step_duration else duration
        await wait_for_release(MEASUREMENT_START, offset=offset)
        messages, payload_bytes = counters()
        deadline = float(Path(MEASUREMENT_START).read_text()) + offset
        lateness = max(0, time.time()-deadline)
        if step_duration:
            STEP_MESSAGES.labels(str(index)).set(messages-previous_messages)
            STEP_BYTES.labels(str(index)).set(payload_bytes-previous_bytes)
            STEP_LATENESS.labels(str(index)).set(lateness)
            if latency:
                current = latency()
                for (kind, bound), value in current.items():
                    difference = value-previous_latency.get((kind,bound),0)
                    if kind == "bucket":
                        STEP_LATENCY_BUCKET.labels(str(index), bound).set(difference)
                    elif kind == "sum":
                        STEP_LATENCY_SUM.labels(str(index)).set(difference)
                    elif kind == "count":
                        STEP_LATENCY_COUNT.labels(str(index)).set(difference)
                previous_latency = current
            previous_messages, previous_bytes = messages, payload_bytes
    MEASUREMENT_END_MESSAGES.set(messages)
    MEASUREMENT_END_BYTES.set(payload_bytes)
    MEASUREMENT_SNAPSHOT_LATENESS.set(lateness)
    MEASUREMENT_SNAPSHOT_COMPLETE.set(1)


async def synchronized_consumer(consumer, duration_seconds=None, consume=None, step_duration=None):
    from src.components.common.prometheus_metrics import COMPONENT_METRICS_FINAL, CONSUMER_RECEIVED_AT_DRAIN
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGUSR1, stop.set)
    loop.add_signal_handler(signal.SIGUSR2,
                            lambda: CONSUMER_RECEIVED_AT_DRAIN.set(consumer.measurement_messages_total))
    consuming = asyncio.create_task(consume() if consume else consumer.consume())
    stopping = asyncio.create_task(stop.wait())
    snapshot = (asyncio.create_task(measurement_snapshot(
        duration_seconds, lambda: (consumer.measurement_messages_total, 0),
        step_duration=step_duration, latency=consumer.prometheus_metrics.snapshot_latency))
        if duration_seconds is not None else None)
    try:
        done, _ = await asyncio.wait((consuming, stopping), return_when=asyncio.FIRST_COMPLETED)
        if consuming in done:
            await consuming
            raise RuntimeError("Consumer stopped before finalization")
        consuming.cancel()
        await asyncio.gather(consuming, return_exceptions=True)
        await consumer.close()
        consumer.prometheus_metrics.flush()
        COMPONENT_METRICS_FINAL.set(1)
        await asyncio.Event().wait()
    finally:
        consuming.cancel()
        stopping.cancel()
        if snapshot:
            snapshot.cancel()
            await asyncio.gather(snapshot, return_exceptions=True)
        await asyncio.gather(consuming, stopping, return_exceptions=True)
        loop.remove_signal_handler(signal.SIGUSR1)
        loop.remove_signal_handler(signal.SIGUSR2)


async def wait_for_release(
        path: str,
        offset: float = 0,
        poll_interval_seconds: float = 0.01,
) -> None:
    """Wait for the orchestrator's scheduled release on the Docker host clock."""
    if offset < 0:
        raise ValueError("offset must be non-negative")
    if poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be greater than zero")

    marker = Path(path)
    while True:
        try:
            release_at = float(marker.read_text())
        except (FileNotFoundError, ValueError):
            # Docker may expose the file before its timestamp is fully written.
            await asyncio.sleep(poll_interval_seconds)
        else:
            await asyncio.sleep(max(0.0, release_at + offset - time.time()))
            return
