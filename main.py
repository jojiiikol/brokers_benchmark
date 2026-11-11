import asyncio
import logging

from src import setup_logging
from src.orchestrator import Orchestrator
from src.scenario_service.dto.broker_config import BrokerConfig, BrokerConnection, Brokers
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.interaction_mode import InteractionMode
from src.scenario_service.dto.producer_config import GuaranteeType, LoadType, ProducerConfig
from src.scenario_service.dto.scenario_config import ScenarioConfig

logger = logging.getLogger(__name__)


async def main() -> None:
    setup_logging()
    logger.info("Starting benchmark orchestrator")
    orchestrator = Orchestrator()
    await orchestrator.docker_service.open_client()
    try:
        await orchestrator.create_scenario(
            ScenarioConfig(
                broker_config=BrokerConfig(
                    broker_type=Brokers.RABBITMQ
                ),
                consumer_config=ConsumerConfig(
                    count=5,
                    docker_args=[],
                    broker_connection=BrokerConnection(
                        broker_type=Brokers.RABBITMQ,
                        username="",
                        password="",
                        host="",
                        port=0,
                    ),
                ),
                producer_config=ProducerConfig(
                    load_type=LoadType.LINEAR,
                    payload_size=128,
                    rps=100,
                    end_rps=1000000,
                    step_duration_seconds=10,
                    guarantee_type=GuaranteeType.AT_MOST_ONCE,
                    count=5,
                    max_in_flight=500,
                    docker_args=[],
                    broker_connection=BrokerConnection(
                        broker_type=Brokers.RABBITMQ,
                        username="",
                        password="",
                        host="",
                        port=0,
                    ),
                ),
                warmup_seconds=5,
                duration_seconds=120,
                interaction_mode=InteractionMode.WORK_QUEUE,
            )
        )
        logger.info("Static benchmark scenario submitted")
        await orchestrator.run()
    finally:
        logger.info("Stopping benchmark orchestrator and cleaning Docker resources")
        await orchestrator.docker_service.clear_resources()
        await orchestrator.docker_service.close_client()


if __name__ == "__main__":
    asyncio.run(main())
