"""Prometheus adapter for the orchestrator.

The orchestrator depends only on an observer interface. This module contains
all Prometheus-specific state, including Docker resource collection, and can
be omitted entirely when metrics are disabled.
"""

import os
from typing import Any

from prometheus_client import Gauge, start_http_server


_ORCHESTRATOR_UP = Gauge(
    "benchmark_orchestrator_up",
    "Whether the benchmark orchestrator process is running.",
)
_ORCHESTRATOR_ACTIVE_TASKS = Gauge(
    "benchmark_orchestrator_active_scenario_tasks",
    "Scenario tasks currently tracked by the orchestrator.",
)
_SCENARIOS_BY_STATUS = Gauge(
    "benchmark_scenarios",
    "Scenarios known to the orchestrator grouped by current status.",
    ("status",),
)
_SCENARIO_STATUS = Gauge(
    "benchmark_scenario_status",
    "Current status of each scenario; exactly one status label is 1.",
    ("scenario_id", "status"),
)
_CONTAINER_CPU_SECONDS = Gauge(
    "benchmark_container_cpu_seconds",
    "CPU time consumed by a benchmark container, reported by Docker.",
    ("scenario_id", "component", "container_name"),
)
_CONTAINER_MEMORY_BYTES = Gauge(
    "benchmark_container_memory_bytes",
    "Memory used by a benchmark container, reported by Docker.",
    ("scenario_id", "component", "container_name"),
)
_CONTAINER_NETWORK_RECEIVED_BYTES = Gauge(
    "benchmark_container_network_received_bytes",
    "Network bytes received by a benchmark container, reported by Docker.",
    ("scenario_id", "component", "container_name"),
)
_CONTAINER_NETWORK_TRANSMITTED_BYTES = Gauge(
    "benchmark_container_network_transmitted_bytes",
    "Network bytes transmitted by a benchmark container, reported by Docker.",
    ("scenario_id", "component", "container_name"),
)
_CONTAINER_DISK_WRITTEN_BYTES = Gauge(
    "benchmark_container_disk_written_bytes",
    "Disk bytes written by a benchmark container, reported by Docker.",
    ("scenario_id", "component", "container_name"),
)


class PrometheusOrchestratorObserver:
    """Updates Prometheus from scenario state and Docker resource snapshots."""

    _STATUSES = (
        "CREATED", "INITIALIZING", "PENDING", "STARTING", "WARMING_UP",
        "RUNNING", "STOPPING", "ERROR", "FINISHED",
    )

    def __init__(self, docker_service: Any) -> None:
        self._docker_service = docker_service
        self._previous_statuses: dict[str, str] = {}

    async def update(self, scenarios: list, active_task_count: int) -> None:
        _ORCHESTRATOR_UP.set(1)
        _ORCHESTRATOR_ACTIVE_TASKS.set(active_task_count)
        self._update_scenario_statuses(scenarios)
        self._update_container_resources(
            await self._docker_service.get_benchmark_container_stats()
        )

    def _update_scenario_statuses(self, scenarios: list) -> None:
        counts: dict[str, int] = {}
        for scenario in scenarios:
            status = scenario.status.value
            counts[status] = counts.get(status, 0) + 1
            previous_status = self._previous_statuses.get(scenario.id)
            if previous_status is not None and previous_status != status:
                _SCENARIO_STATUS.labels(
                    scenario_id=scenario.id, status=previous_status
                ).set(0)
            _SCENARIO_STATUS.labels(scenario_id=scenario.id, status=status).set(1)
            self._previous_statuses[scenario.id] = status

        for status in self._STATUSES:
            _SCENARIOS_BY_STATUS.labels(status=status).set(counts.get(status, 0))

    @staticmethod
    def _update_container_resources(containers: list[dict]) -> None:
        for container in containers:
            labels = {
                "scenario_id": container["scenario_id"],
                "component": container["component"],
                "container_name": container["container_name"],
            }
            _CONTAINER_CPU_SECONDS.labels(**labels).set(container["cpu_seconds"])
            _CONTAINER_MEMORY_BYTES.labels(**labels).set(container["memory_bytes"])
            _CONTAINER_NETWORK_RECEIVED_BYTES.labels(**labels).set(
                container["network_received_bytes"]
            )
            _CONTAINER_NETWORK_TRANSMITTED_BYTES.labels(**labels).set(
                container["network_transmitted_bytes"]
            )
            _CONTAINER_DISK_WRITTEN_BYTES.labels(**labels).set(
                container["disk_written_bytes"]
            )


def metrics_enabled() -> bool:
    """Return whether Prometheus integration is enabled for this process."""
    return os.getenv("BENCHMARK_METRICS_ENABLED", "true").lower() not in {
        "0", "false", "no", "off",
    }


def start_prometheus_server(port: int = 8000) -> None:
    """Expose only orchestrator metrics from this process."""
    start_http_server(port, addr="0.0.0.0")
