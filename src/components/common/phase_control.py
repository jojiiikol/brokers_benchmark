"""File-based phase synchronization shared with benchmark containers."""

import asyncio
import time
from pathlib import Path


READY = "/tmp/benchmark-ready"
WARMUP_START = "/tmp/benchmark-warmup-start"
WARMUP_DONE = "/tmp/benchmark-warmup-done"
MEASUREMENT_START = "/tmp/benchmark-measurement-start"


async def wait_for_release(
        path: str,
        offset_seconds: float = 0,
        poll_interval_seconds: float = 0.01,
) -> None:
    """Wait for an orchestrator release file and its wall-clock deadline."""
    if offset_seconds < 0:
        raise ValueError("offset_seconds must be non-negative")
    if poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be greater than zero")

    release_file = Path(path)
    while True:
        try:
            release_at = float(release_file.read_text(encoding="utf-8"))
        except FileNotFoundError:
            await asyncio.sleep(poll_interval_seconds)
        except ValueError as error:
            raise RuntimeError(f"Invalid phase release timestamp in {path}") from error
        else:
            break

    await asyncio.sleep(max(0, release_at + offset_seconds - time.time()))
