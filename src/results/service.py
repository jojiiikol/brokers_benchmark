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
            f'sum(benchmark_measurement_end_messages{{scenario_id="{scenario_id}",component="consumer"}})'
        ))

    async def boundary_rates(self, scenario, step_index=None):
        selector = f'scenario_id="{scenario.id}"'
        is_step = step_index is not None
        if is_step:
            selector += f',step="{step_index}"'
        message_metric = "benchmark_measurement_step_messages" if is_step else "benchmark_measurement_end_messages"
        byte_metric = "benchmark_measurement_step_payload_bytes" if is_step else "benchmark_measurement_end_payload_bytes"
        complete_metric = message_metric if is_step else "benchmark_measurement_snapshot_complete"
        lateness_metric = "benchmark_measurement_step_lateness_seconds" if is_step else "benchmark_measurement_snapshot_lateness_seconds"
        def query(metric, role, function="sum"):
            return f'{function}({metric}{{{selector},component="{role}"}})'
        producer_count, consumer_count, sent, received, payload, lateness = await asyncio.gather(
            self._query(f'count({complete_metric}{{{selector},component="producer"}}' + (')' if is_step else ' == 1)')),
            self._query(f'count({complete_metric}{{{selector},component="consumer"}}' + (')' if is_step else ' == 1)')),
            self._query(query(message_metric, "producer")),
            self._query(query(message_metric, "consumer")),
            self._query(query(byte_metric, "producer")),
            self._query(f'max({lateness_metric}{{{selector}}})'),
        )
        duration = scenario.config.producer_config.step_duration_seconds if is_step else scenario.config.duration_seconds
        producer_ok = producer_count == scenario.config.producer_config.count
        consumer_ok = consumer_count == scenario.config.consumer_config.count
        return dict(producer_messages_per_second=sent/duration if producer_ok and sent is not None else None,
                    consumer_messages_per_second=received/duration if consumer_ok and received is not None else None,
                    producer_payload_bytes_per_second=payload/duration if producer_ok and payload is not None else None,
                    consumer_payload_bytes_per_second=(received*scenario.config.producer_config.payload_size*1024/duration
                                                       if consumer_ok and received is not None else None)), lateness

    async def step_latency(self, scenario_id, index):
        selector = f'scenario_id="{scenario_id}",step="{index}"'
        count, total, *quantiles = await asyncio.gather(
            self._query(f'sum(benchmark_measurement_step_latency_count{{{selector}}})'),
            self._query(f'sum(benchmark_measurement_step_latency_sum{{{selector}}})'),
            *(self._query(f'histogram_quantile({q/100}, sum by (le) (benchmark_measurement_step_latency_bucket{{{selector}}}))')
              for q in (50,75,95,99)),
        )
        return dict(samples=_as_int(count), average=total/count if count and total is not None else None,
                    **dict(zip(("p50","p75","p95","p99"),quantiles)))

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

    async def _range(self, query: str, start: float, end: float):
        try:
            response = await asyncio.to_thread(
                requests.get, f"{self._base_url}/api/v1/query_range",
                params=dict(query=query, start=start, end=end, step=5),
                timeout=10,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("status") != "success":
                return []
            return payload.get("data", {}).get("result", [])
        except (requests.RequestException, ValueError, KeyError):
            logger.warning("Could not collect measurement interval from Prometheus")
            return []

    async def interval(self, scenario_id: str, start: float, end: float):
        """Scrape-resolution estimates; never substitute missing samples with zero."""
        selector = f'scenario_id="{scenario_id}"'
        cpu, memory = await asyncio.gather(
            self._range(f'sum by (component) (benchmark_container_cpu_seconds{{{selector}}})', start, end),
            self._range(f'sum by (component) (benchmark_container_memory_bytes{{{selector}}})', start, end),
        )
        resources = {role: dict(cpu_cores_average=None, cpu_cores_maximum=None,
                                memory_bytes_average=None, memory_bytes_maximum=None)
                     for role in ("broker", "producer", "consumer")}
        for series in cpu:
            role = series.get("metric", {}).get("component")
            points = _finite_points(series.get("values", []))
            slopes = [(b[1]-a[1])/(b[0]-a[0]) for a,b in zip(points, points[1:])
                      if b[0] > a[0] and b[1] >= a[1]]
            if role in resources and slopes:
                resources[role].update(cpu_cores_average=(points[-1][1]-points[0][1])/(points[-1][0]-points[0][0]),
                                       cpu_cores_maximum=max(slopes))
        for series in memory:
            role = series.get("metric", {}).get("component")
            points = _finite_points(series.get("values", []))
            if role in resources and len(points) > 1:
                weighted = sum((b[0]-a[0]) * a[1] for a,b in zip(points, points[1:]))
                resources[role].update(memory_bytes_average=weighted/(points[-1][0]-points[0][0]),
                                       memory_bytes_maximum=max(v for _,v in points))
        return dict(resources=resources)


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
        measurement_start = scenario.measurement_started_at
        measurement_end = getattr(scenario, "measurement_finished_at", None)
        if measurement_start and measurement_end and hasattr(self._collector, "interval"):
            interval = await self._collector.interval(scenario.id, measurement_start.timestamp(), measurement_end.timestamp())
            metrics.update(interval)
            if scenario.config.producer_config.load_type.value == "linear":
                producer = scenario.config.producer_config
                step = producer.step_duration_seconds
                count = round(scenario.config.duration_seconds / step)
                metrics["steps"] = []
                for index in range(count):
                    start = measurement_start.timestamp() + index*step
                    step_metrics = await self._collector.interval(scenario.id, start, start+step)
                    step_metrics["rates"], lateness = await self._collector.boundary_rates(scenario, index)
                    step_metrics["latency_seconds"] = await self._collector.step_latency(scenario.id, index)
                    metrics["steps"].append(dict(index=index, target_rps_per_producer=round(
                        producer.rps+(producer.end_rps-producer.rps)*index/(count-1)),
                        started_at=start, ended_at=start+step,
                        snapshot_lateness_seconds=lateness, metrics=step_metrics))
        quality = dict(final_metrics_collected=scenario.final_metrics_collected is True,
                       interval_source="prometheus_scrape_resolution", excluded_metrics={}, warnings=[])
        metrics["quality"] = quality
        for index, step in enumerate(metrics.get("steps", [])):
            if (step.get("snapshot_lateness_seconds") or 0) > .1:
                for section in ("rates", "latency_seconds"):
                    for metric in step["metrics"][section]:
                        quality["excluded_metrics"][f"step_{index}.{section}.{metric}"] = "measurement_boundary_timer_delayed"
        if hasattr(self._collector, "boundary_rates") and measurement_end:
            metrics["rates"], lateness = await self._collector.boundary_rates(scenario)
            quality.update(rates_source="measurement_deadline_snapshot", snapshot_lateness_seconds=lateness)
            if lateness is not None and lateness > .1:
                quality["warnings"].append("measurement_boundary_timer_delayed")
                for metric in metrics["rates"]:
                    quality["excluded_metrics"][f"rates.{metric}"] = "measurement_boundary_timer_delayed"
        accepted = metrics.get("messages", {}).get("producer_accepted")
        received = metrics.get("messages", {}).get("consumer_received")
        duplicates = metrics.get("messages", {}).get("consumer_duplicates")
        expected = (accepted * scenario.config.consumer_config.count
                    if accepted is not None and scenario.config.interaction_mode is InteractionMode.PUB_SUB else accepted)
        accounting_complete = (expected is not None and received is not None and duplicates is not None
                               and received-duplicates >= expected)
        if scenario.drain_completed is None:
            quality["delivery_accounting_complete"] = accounting_complete
            metrics["late_deliveries"] = None
            if scenario.final_metrics_collected and hasattr(self._collector, "received_at_drain"):
                before = await self._collector.received_at_drain(scenario.id)
                metrics["late_deliveries"] = received-before if received is not None and before is not None else None
        if not accounting_complete and scenario.drain_completed is not True:
            for key in ("lost_messages", "lost_share_percent"):
                quality["excluded_metrics"][f"reliability.{key}"] = "delivery_accounting_incomplete"
                if "reliability" in metrics:
                    metrics["reliability"][key] = None
        target = scenario.config.producer_config.rps
        if scenario.config.producer_config.load_type.value == "linear":
            target = (target + scenario.config.producer_config.end_rps)/2
        actual = metrics.get("rates", {}).get("producer_messages_per_second")
        if target and actual is not None and actual < target*scenario.config.producer_config.count*.95:
            quality["warnings"].append("target_rps_not_reached")
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
                "measurement_finished_at": _serialize(measurement_end),
                "drain_started_at": _serialize(scenario.drain_started_at),
                "finished_at": _serialize(scenario.finished_at),
                "configuration": _serialize(scenario.config),
            },
            "metrics": metrics,
        }
        for metric in quality["excluded_metrics"]:
            if metric.startswith("reliability."):
                metrics.get("reliability", {})[metric.split(".", 1)[1]] = None
        if getattr(scenario, "experiment", None):
            artifact["experiment"] = scenario.experiment
        self._directory.mkdir(parents=True, exist_ok=True)
        path = self._directory / f"scenario-{scenario.id}.json"
        from src.experiments.storage import atomic_json
        atomic_json(path, artifact)
        logger.info("Saved scenario result to %s", path)
        return path


def _as_int(value: float | None) -> int | None:
    return None if value is None else int(value)


def _finite_points(values):
    points = []
    for timestamp, value in values:
        number = float(value)
        if math.isfinite(number):
            points.append((float(timestamp), number))
    return points



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
