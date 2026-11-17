"""Collect and persist a self-contained JSON artifact for each scenario."""

import asyncio
import dataclasses
import datetime
import enum
import json
import logging
import math
import os
from pathlib import Path
from typing import Any

import requests

from src.scenario_service.dto.interaction_mode import InteractionMode


logger = logging.getLogger(__name__)


class PrometheusScenarioResultsCollector:
    """Read the final scenario counters and latency summary from Prometheus."""

    def __init__(self, base_url: str | None = None) -> None:
        self._base_url = base_url or os.getenv(
            "PROMETHEUS_URL", "http://prometheus:9090"
        )

    async def collect(self, scenario_id: str) -> dict[str, Any]:
        selector = f'scenario_id="{scenario_id}"'
        sampled = f"benchmark_consumer_latency_seconds_count{{{selector}}} > 0"
        (
            sent,
            producer_errors,
            payload_bytes,
            received,
            duplicates,
            sample_count,
            latency_sum,
            latency_min,
            latency_max,
            p50,
            p75,
            p95,
            p99,
        ) = await asyncio.gather(
            self._query(f"sum(benchmark_producer_messages_accepted_total{{{selector},phase=\"measurement\"}})"),
            self._query(f"sum(benchmark_producer_publish_errors_total{{{selector},phase=\"measurement\"}})"),
            self._query(f"sum(benchmark_producer_payload_bytes_total{{{selector},phase=\"measurement\"}})"),
            self._query(f"sum(benchmark_consumer_messages_received_total{{{selector},phase=\"measurement\"}})"),
            self._query(f"sum(benchmark_consumer_duplicate_messages_total{{{selector},phase=\"measurement\"}})"),
            self._query(f"sum(benchmark_consumer_latency_seconds_count{{{selector}}})"),
            self._query(f"sum(benchmark_consumer_latency_seconds_sum{{{selector}}})"),
            self._query(f"min(benchmark_consumer_latency_seconds_min{{{selector}}} and ({sampled}))"),
            self._query(f"max(benchmark_consumer_latency_seconds_max{{{selector}}} and ({sampled}))"),
            self._quantile(0.50, selector),
            self._quantile(0.75, selector),
            self._quantile(0.95, selector),
            self._quantile(0.99, selector),
        )

        return {
            "source": "prometheus",
            "messages": {
                "producer_accepted": _as_int(sent),
                "producer_errors": _as_int(producer_errors),
                "producer_payload_bytes": _as_int(payload_bytes),
                "consumer_received": _as_int(received),
                "consumer_duplicates": _as_int(duplicates),
            },
            "reliability": _reliability(sent, received, duplicates),
            "latency_seconds": {
                "samples": _as_int(sample_count),
                "minimum": latency_min,
                "maximum": latency_max,
                "average": (
                    latency_sum / sample_count
                    if latency_sum is not None and sample_count not in (None, 0)
                    else None
                ),
                "p50": _bounded_quantile(p50, latency_min, latency_max),
                "p75": _bounded_quantile(p75, latency_min, latency_max),
                "p95": _bounded_quantile(p95, latency_min, latency_max),
                "p99": _bounded_quantile(p99, latency_min, latency_max),
            },
        }

    async def _quantile(self, quantile: float, selector: str) -> float | None:
        return await self._query(
            "histogram_quantile("
            f"{quantile}, sum by (le) "
            f"(benchmark_consumer_latency_seconds_bucket{{{selector}}}))"
        )

    async def wait_for_final_metrics(self, scenario_id: str, expected: int, timeout: float) -> bool:
        query = f'count(benchmark_component_metrics_final{{scenario_id="{scenario_id}"}} == 1)'
        try:
            async with asyncio.timeout(timeout):
                while True:
                    if await self._query(query) == expected:
                        return True
                    await asyncio.sleep(0.5)
        except TimeoutError:
            logger.warning("Final Prometheus scrape incomplete for scenario %s", scenario_id)
            return False

    async def received_at_drain(self, scenario_id: str) -> int | None:
        return _as_int(await self._query(
            f'sum(benchmark_consumer_received_at_drain{{scenario_id="{scenario_id}",component="consumer"}})'
        ))

    async def _query(self, query: str) -> float | None:
        try:
            response = await asyncio.to_thread(
                requests.get,
                f"{self._base_url}/api/v1/query",
                params={"query": query},
                timeout=3,
            )
            response.raise_for_status()
            payload = response.json()
            result = payload.get("data", {}).get("result", [])
            if not result:
                return None
            value = float(result[0]["value"][1])
            return value if math.isfinite(value) else None
        except (requests.RequestException, KeyError, TypeError, ValueError):
            logger.warning("Could not collect scenario results from Prometheus")
            return None


class ScenarioResultService:
    """Write one durable JSON result file per completed experiment."""

    def __init__(
        self,
        directory: Path | None = None,
        collector: PrometheusScenarioResultsCollector | None = None,
    ) -> None:
        self._directory = directory or Path(
            os.getenv("BENCHMARK_RESULTS_DIR", "/app/results")
        )
        self._collector = collector or PrometheusScenarioResultsCollector()

    async def wait_for_final_metrics(self, scenario_id: str, expected: int, timeout: float) -> bool:
        return await self._collector.wait_for_final_metrics(scenario_id, expected, timeout)

    async def save(self, scenario: Any, final_status: str) -> Path:
        metrics = await self._collector.collect(scenario.id)
        if scenario.config.interaction_mode is InteractionMode.PUB_SUB:
            messages = metrics["messages"]
            sent = messages.get("producer_accepted")
            expected_deliveries = (
                sent * scenario.config.consumer_config.count if sent is not None else None
            )
            metrics["reliability"] = {
                "expected_deliveries": expected_deliveries,
                **_reliability(
                    expected_deliveries,
                    messages.get("consumer_received"),
                    messages.get("consumer_duplicates"),
                ),
            }
        if scenario.drain_completed is not None:
            received = metrics["messages"]["consumer_received"]
            received_at_drain = (
                await self._collector.received_at_drain(scenario.id)
                if scenario.final_metrics_collected else None
            )
            metrics["drain"] = {
                "completed": scenario.drain_completed,
                "pending_at_last_check": scenario.drain_pending_messages,
                "final_metrics_collected": scenario.final_metrics_collected,
                "late_deliveries": (
                    received - received_at_drain
                    if received is not None and received_at_drain is not None else None
                ),
            }
            if not scenario.drain_completed or not scenario.final_metrics_collected:
                metrics["reliability"]["lost_messages"] = None
                metrics["reliability"]["lost_share_percent"] = None
        artifact = {
            "schema_version": 1,
            "scenario": {
                "id": scenario.id,
                "name": scenario.name,
                "status": final_status,
                "created_at": _serialize(scenario.created_at),
                "started_at": _serialize(scenario.started_at),
                "warmup_finished_at": _serialize(scenario.warmup_finished_at),
                "measurement_started_at": _serialize(scenario.measurement_started_at),
                "drain_started_at": _serialize(scenario.drain_started_at),
                "finished_at": _serialize(scenario.finished_at),
                "configuration": _serialize(scenario.config),
            },
            "metrics": metrics,
        }
        self._directory.mkdir(parents=True, exist_ok=True)
        path = self._directory / f"scenario-{scenario.id}.json"
        path.write_text(
            json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        logger.info("Saved scenario result to %s", path)
        return path


def _as_int(value: float | None) -> int | None:
    return None if value is None else int(value)


def _bounded_quantile(
        estimate: float | None, observed_min: float | None, observed_max: float | None,
) -> float | None:
    if estimate is None or observed_min is None or observed_max is None:
        return estimate
    # A bucket interpolation can fall outside the observed latency range.
    return min(observed_max, max(observed_min, estimate))


def _reliability(
        sent: float | None,
        received: float | None,
        duplicates: float | None,
) -> dict[str, int | float | None]:
    """Calculate delivery ratios from producer and consumer counters."""
    if sent is None or received is None or duplicates is None:
        return {
            "unique_received": None,
            "lost_messages": None,
            "lost_share_percent": None,
            "duplicate_share_percent": None,
        }
    unique_received = max(0, int(received - duplicates))
    lost_messages = max(0, int(sent - unique_received))
    return {
        "unique_received": unique_received,
        "lost_messages": lost_messages,
        "lost_share_percent": lost_messages / sent * 100 if sent else 0.0,
        "duplicate_share_percent": duplicates / received * 100 if received else 0.0,
    }


def _serialize(value: Any) -> Any:
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if dataclasses.is_dataclass(value):
        return {
            field.name: _serialize(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    if isinstance(value, dict):
        return {str(key): _serialize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize(item) for item in value]
    return value
