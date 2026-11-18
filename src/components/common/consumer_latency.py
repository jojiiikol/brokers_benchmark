"""Shared consumer-side latency sampling and log-window configuration."""

import argparse
import math


DEFAULT_METRICS_WINDOW_SIZE = 1_000
DEFAULT_LATENCY_SAMPLE_EVERY = 10
DEFAULT_METRICS_LOG_EVERY_WINDOWS = 100


class LatencyMetrics:
    """In-memory percentile aggregation for one reporting window."""

    PERCENTILES = (50, 75, 95, 99)

    def __init__(self) -> None:
        self._values_ms: list[float] = []

    def observe(self, latency_ms: float) -> None:
        self._values_ms.append(latency_ms)

    @property
    def count(self) -> int:
        return len(self._values_ms)

    def summary(self) -> dict[str, float | int | None]:
        result: dict[str, float | int | None] = {"count": self.count}
        for percentile in self.PERCENTILES:
            result[f"p{percentile}_ms"] = self._percentile(percentile)
        return result

    def _percentile(self, percentile: int) -> float | None:
        if not self._values_ms:
            return None
        values = sorted(self._values_ms)
        rank = (len(values) - 1) * percentile / 100
        lower = math.floor(rank)
        upper = math.ceil(rank)
        if lower == upper:
            return values[lower]
        return values[lower] + (values[upper] - values[lower]) * (rank - lower)


def add_consumer_latency_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--metrics-window-size",
        type=int,
        default=DEFAULT_METRICS_WINDOW_SIZE,
        help="Number of sampled latencies in one reporting window",
    )
    parser.add_argument(
        "--latency-sample-every",
        type=int,
        default=DEFAULT_LATENCY_SAMPLE_EVERY,
        help="Observe latency for every Nth measurement message",
    )
    parser.add_argument(
        "--metrics-log-every-windows",
        type=int,
        default=DEFAULT_METRICS_LOG_EVERY_WINDOWS,
        help="Log one latency summary after this many completed windows",
    )
