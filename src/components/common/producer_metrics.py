"""Local producer metrics that do not query or alter a broker."""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol


class ProducerMetricsSink(Protocol):
    def accepted(self, phase: str, payload_bytes: int) -> None: ...
    def failed(self, phase: str) -> None: ...
    def set_in_flight(self, count: int) -> None: ...

@dataclass
class _Counters:
    attempts: int = 0
    accepted: int = 0
    errors: int = 0
    payload_bytes: int = 0

    def difference(self, previous: "_Counters") -> "_Counters":
        return _Counters(
            attempts=self.attempts - previous.attempts,
            accepted=self.accepted - previous.accepted,
            errors=self.errors - previous.errors,
            payload_bytes=self.payload_bytes - previous.payload_bytes,
        )


class ProducerMetrics:
    """Collect producer-side hand-off counters and periodically log them.

    ``accepted`` means that the client library accepted the payload locally.
    In at-most-once mode it deliberately does not mean broker confirmation or
    consumer delivery.
    """

    def __init__(
            self,
            logger: logging.Logger,
            report_interval_seconds: float = 5,
            sink: ProducerMetricsSink | None = None,
    ) -> None:
        if report_interval_seconds <= 0:
            raise ValueError("metrics_report_interval_seconds must be greater than zero")
        self._logger = logger
        self._report_interval_seconds = report_interval_seconds
        self._phase: str | None = None
        self._total = _Counters()
        self._previous_total = _Counters()
        self._last_report_at: float | None = None
        self._in_flight = 0
        self._max_in_flight = 0
        self._sink = sink

    def begin_phase(self, phase: str) -> None:
        self._phase = phase
        self._total = _Counters()
        self._previous_total = _Counters()
        self._last_report_at = time.monotonic()
        self._in_flight = 0
        self._max_in_flight = 0

    async def track(
            self,
            payload_bytes: int,
            publish: Callable[[], Awaitable[None]],
    ) -> None:
        self._require_phase()
        self._increment("attempts", 1)
        try:
            await publish()
        except Exception:
            self._increment("errors", 1)
            if self._sink is not None:
                self._sink.failed(self._phase)
            self._report_if_due()
            raise
        else:
            self._increment("accepted", 1)
            self._increment("payload_bytes", payload_bytes)
            if self._sink is not None:
                self._sink.accepted(self._phase, payload_bytes)
            self._report_if_due()

    def set_in_flight(self, count: int) -> None:
        self._require_phase()
        self._in_flight = count
        self._max_in_flight = max(self._max_in_flight, count)
        if self._sink is not None:
            self._sink.set_in_flight(count)
        self._report_if_due()

    def log_final(self) -> None:
        self._require_phase()
        self._log_window(time.monotonic(), final=True)

    def _increment(self, field: str, value: int) -> None:
        setattr(self._total, field, getattr(self._total, field) + value)

    def _report_if_due(self) -> None:
        now = time.monotonic()
        if self._last_report_at is None:
            self._last_report_at = now
            return
        if now - self._last_report_at >= self._report_interval_seconds:
            self._log_window(now, final=False)

    def _log_window(self, now: float, *, final: bool) -> None:
        if self._last_report_at is None:
            return
        elapsed = now - self._last_report_at
        if elapsed <= 0:
            return

        window = self._total.difference(self._previous_total)
        prefix = "Final producer metrics" if final else "Producer metrics"
        self._logger.info(
            "%s | phase=%s | window_seconds=%.3f | attempts=%s | "
            "accepted=%s | accepted_rps=%.3f | payload_mib=%.3f | "
            "errors=%s | in_flight=%s | max_in_flight=%s",
            prefix,
            self._phase,
            elapsed,
            window.attempts,
            window.accepted,
            window.accepted / elapsed,
            window.payload_bytes / (1024 * 1024),
            window.errors,
            self._in_flight,
            self._max_in_flight,
        )
        self._previous_total = _Counters(**vars(self._total))
        self._last_report_at = now

    def _require_phase(self) -> None:
        if self._phase is None:
            raise RuntimeError("Producer metrics phase is not initialized")
