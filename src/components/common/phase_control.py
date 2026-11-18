"""Signals shared by the orchestrator and synchronized benchmark clients."""

import asyncio
from pathlib import Path
import time

READY = "/tmp/benchmark-ready"
WARMUP_START = "/tmp/benchmark-warmup-start"
WARMUP_DONE = "/tmp/benchmark-warmup-done"
MEASUREMENT_START = "/tmp/benchmark-measurement-start"


async def wait_for_release(path: str, offset: float = 0) -> None:
    """Wait for the orchestrator's scheduled release on the Docker host clock."""
    marker = Path(path)
    while True:
        try:
            release_at = float(marker.read_text())
        except (FileNotFoundError, ValueError):
            await asyncio.sleep(0.01)
        else:
            await asyncio.sleep(max(0.0, release_at + offset - time.time()))
            return
