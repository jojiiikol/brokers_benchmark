"""Signals shared by the orchestrator and synchronized benchmark clients."""

import asyncio
from pathlib import Path
import time

READY = "/tmp/benchmark-ready"
WARMUP_START = "/tmp/benchmark-warmup-start"
WARMUP_DONE = "/tmp/benchmark-warmup-done"
MEASUREMENT_START = "/tmp/benchmark-measurement-start"


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
