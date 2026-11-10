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
                broker_config=BrokerConfig(broker_type=Brokers.NATS_PULL),
                consumer_config=ConsumerConfig(
                    count=1,
                    docker_args=[],
                    broker_connection=BrokerConnection(
                        broker_type=Brokers.NATS_PULL,
                        username="",
                        password="",
                        host="",
                        port=0,
                    ),
                ),
                producer_config=ProducerConfig(
                    load_type=LoadType.linear,
                    payload_size=512,
                    rps=1000,
                    guarantee_type=GuaranteeType.EXACTLY_ONCE,
                    count=1,
                    docker_args=[],
                    broker_connection=BrokerConnection(
                        broker_type=Brokers.NATS_PULL,
                        username="",
                        password="",
                        host="",
                        port=0,
                    ),
                ),
                warmup_seconds=5,
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
