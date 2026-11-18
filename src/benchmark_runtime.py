"""Shared runtime for benchmark-service entrypoints."""

import asyncio
import dataclasses
import logging
import signal

from src import setup_logging
from src.observability.prometheus import (
    PrometheusOrchestratorObserver,
    metrics_enabled,
    start_prometheus_server,
)
from src.orchestrator import Orchestrator
from src.scenario_service.dto.scenario_config import ScenarioConfig


logger = logging.getLogger(__name__)


async def run_scenario(
        scenario_config: ScenarioConfig,
        *,
        stop_when_complete: bool,
        cleanup_all_resources: bool,
) -> None:
    """Submit one scenario and manage its Docker lifecycle."""
    setup_logging()
    logger.info(
        "Starting benchmark orchestrator with scenario: %s",
        dataclasses.asdict(scenario_config),
    )
    orchestrator = Orchestrator()
    if metrics_enabled():
        start_prometheus_server()
        orchestrator.set_observer(
            PrometheusOrchestratorObserver(orchestrator.docker_service)
        )
    else:
        logger.info("Prometheus integration is disabled")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals: list[signal.Signals] = []
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(shutdown_signal, stop_event.set)
            registered_signals.append(shutdown_signal)
        except NotImplementedError:
            logger.warning("Signal handlers are not available on this platform")

    await orchestrator.docker_service.open_client()
    try:
        await orchestrator.create_scenario(scenario_config)
        logger.info("Benchmark scenario submitted")
        await orchestrator.run(
            stop_event,
            stop_when_complete=stop_when_complete,
            clear_all_resources_on_shutdown=cleanup_all_resources,
        )
    finally:
        logger.info("Stopping benchmark orchestrator and cleaning Docker resources")
        await orchestrator.shutdown(clear_all_resources=cleanup_all_resources)
        await orchestrator.docker_service.close_client()
        for shutdown_signal in registered_signals:
            loop.remove_signal_handler(shutdown_signal)
