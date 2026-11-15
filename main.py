import asyncio
import logging
import signal

from src import setup_logging
from src.orchestrator import Orchestrator
from src.observability.prometheus import (
    PrometheusOrchestratorObserver,
    metrics_enabled,
    start_prometheus_server,
)
from src.scenario_service.dto.broker_config import BrokerConfig, Brokers
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.interaction_mode import InteractionMode
from src.scenario_service.dto.producer_config import GuaranteeType, LoadType, ProducerConfig
from src.scenario_service.dto.scenario_config import ScenarioConfig

logger = logging.getLogger(__name__)


async def main() -> None:
    setup_logging()
    logger.info("Starting benchmark orchestrator")
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
        await orchestrator.create_scenario(
            ScenarioConfig(
                broker_config=BrokerConfig(
                    broker_type=Brokers.RABBITMQ
                ),
                consumer_config=ConsumerConfig(
                    count=1,
                ),
                producer_config=ProducerConfig(
                    load_type=LoadType.LINEAR,
                    payload_size=64,
                    rps=100,
                    end_rps=100000,
                    step_duration_seconds=10,
                    guarantee_type=GuaranteeType.AT_MOST_ONCE,
                    count=1,
                    max_in_flight=500,
                ),
                warmup_seconds=5,
                duration_seconds=120,
                interaction_mode=InteractionMode.WORK_QUEUE,
            )
        )
        logger.info("Static benchmark scenario submitted")
        await orchestrator.run(stop_event)
    finally:
        logger.info("Stopping benchmark orchestrator and cleaning Docker resources")
        await orchestrator.shutdown()
        await orchestrator.docker_service.close_client()
        for shutdown_signal in registered_signals:
            loop.remove_signal_handler(shutdown_signal)


if __name__ == "__main__":
    asyncio.run(main())
