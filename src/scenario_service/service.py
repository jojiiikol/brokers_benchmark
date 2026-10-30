import asyncio
import dataclasses
import datetime
import enum
import logging
import os
import pathlib
import uuid

from src.config import CONTAINERS_NAMESPACE
from src.docker_configurator import ContainerConfig
from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.creator.base.abstract_broker import BaseBrokerCreator
from src.scenario_service.creator.base.abstract_consumer import BaseConsumerCreator
from src.scenario_service.creator.base.abstract_producer import BaseProducerCreator
# from src.scenario_service.configurator import ComponentConfigurator
from src.scenario_service.creator.base.abstract_service import BaseServiceCreator
from src.scenario_service.creator.brokers.rabbitmq_creator import RabbitMQCreator
from src.scenario_service.creator.consumer.consumer_creator import ConsumerCreator
from src.scenario_service.creator.producer.producer_creator import ProducerCreator
from src.scenario_service.dto.broker_config import BrokerConfig, Brokers
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.producer_config import ProducerConfig, LoadType, GuaranteeType
from src.scenario_service.dto.scenario_config import ScenarioConfig

logger = logging.getLogger("my_app")


COMPONENTS_PATH = pathlib.Path(__file__).resolve().parent.parent / "components"

class ScenarioStatus(enum.Enum):
    CREATED = 'CREATED'
    INITIALIZING = 'INITIALIZING'
    PENDING = 'PENDING'
    STARTING = 'STARTING'
    RUNNING = 'RUNNING'
    STOPPED = 'STOPPED'
    ERROR = 'ERROR'
    COMPLETED = 'COMPLETED'

@dataclasses.dataclass
class Scenario:
    id: str
    name: str
    containers: list
    config: ScenarioConfig
    finished_at: datetime.datetime | None

    status: ScenarioStatus = ScenarioStatus.CREATED
    created_at: datetime.datetime = datetime.datetime.now()

class ScenarioService:
    MAX_RUNNING_SCENARIO_COUNT: int = 5
    TIMEOUT_SCENARIO_SECONDS: int = 45

    def __init__(
            self,
            # configurator: ComponentConfigurator,
            docker_service: SdkDockerService = SdkDockerService(),

    ):
        self.docker_service = docker_service
        # self.configurator = ComponentConfigurator(docker_service)
        self.producer_creator: BaseProducerCreator = ProducerCreator(docker_service)
        self.consumer_creator: BaseConsumerCreator = ConsumerCreator(docker_service)
        self.broker_creator: BaseBrokerCreator = RabbitMQCreator(docker_service)

    def _component_count(
            self,
            component_name: str,
            labels: list[str],
    ) -> int:
        return len(list(filter(lambda item: item.get("component") == component_name, labels)))


    async def initialize_scenario(
            self,
            scenario: Scenario,
    ) -> Scenario:
        scenario_id = scenario.id
        scenario.status = ScenarioStatus.INITIALIZING
        try:

            network = await self.docker_service.create_network(
                name=f"network-{scenario_id}",
                labels={"namespace": CONTAINERS_NAMESPACE},
            )

            broker_container = await self.broker_creator.configurate(
                scenario_id=scenario_id,
                config=scenario.config.broker_config,
            )

            scenario.config.producer_config.docker_args.extend([
                "--broker-host", f"rabbitmq-{scenario_id}",
                "--broker-port", "5672",
            ])

            producer_container = await self.producer_creator.configurate(
                scenario_id=scenario_id,
                config=scenario.config.producer_config,
            )

            scenario.config.consumer_config.docker_args.extend([
                "--broker-host", f"rabbitmq-{scenario_id}",
                "--broker-port", "5672",
            ])

            consumer_container = await self.consumer_creator.configurate(
                scenario_id=scenario_id,
                config=scenario.config.consumer_config,
            )

            await network.connect({"Container": broker_container})
            await network.connect({"Container": producer_container})
            await network.connect({"Container": consumer_container})

            scenario.containers.append(producer_container)
            scenario.containers.append(broker_container)
            scenario.containers.append(consumer_container)

            if not await self._validate_scenario(scenario):
                raise ValueError("Component in scenario not built")

            scenario.status = ScenarioStatus.PENDING
        except Exception as e:
            logger.error(e)
            scenario.status = ScenarioStatus.ERROR

        return scenario

    async def _validate_scenario(self, scenario: Scenario) -> bool:
        expected_counts = {
            "producer": scenario.config.producer_config.count,
            "consumer": scenario.config.consumer_config.count,
            "broker": 1,
        }

        for component, expected_count in expected_counts.items():
            resources = await self.docker_service._get_resources_by_labels(
                resource_collection=self.docker_service.client.containers,
                labels={
                    "namespace": CONTAINERS_NAMESPACE,
                    "component": component,
                    "scenario_id": scenario.id,
                },
                all=True,
            )

            actual_count = len(resources)

            if actual_count != expected_count:
                logger.error(
                    "Invalid component count for %s: expected=%s, actual=%s",
                    component,
                    expected_count,
                    actual_count,
                )
                return False

        return True

    async def create_scenario(
            self,
            scenario_config: ScenarioConfig,
    ) -> Scenario:

        scenario_id = str(uuid.uuid4())
        scenario = Scenario(
            id=scenario_id,
            name="scenario - {}".format(scenario_id),
            containers=[],
            config=scenario_config,
            finished_at=None,
            status=ScenarioStatus.CREATED,
            created_at=datetime.datetime.now(),
        )

        logger.info(f"Created scenario {scenario.id}")
        return scenario


    async def start_scenario(self, scenario: Scenario) -> str | None:
        scenario.status = ScenarioStatus.STARTING
        try:
            container_ids = scenario.containers
            for container_id in container_ids:
                if await self.docker_service.start_container(container_id):
                    raise ValueError("Container not started")

            scenario.status = ScenarioStatus.RUNNING
            return scenario.id
        except Exception as e:
            logger.error(e)
            scenario.status = ScenarioStatus.ERROR




async def main():
    service = ScenarioService()
    scenario = await service.create_scenario(
        scenario_config=ScenarioConfig(
            broker_config=BrokerConfig(
                broker_type=Brokers.RABBITMQ
            ),
            consumer_config=ConsumerConfig(
                count=2,
                docker_args=[]
            ),
            producer_config=ProducerConfig(
                load_type=LoadType.linear,
                message_size=1024,
                speed=2,
                guarantee_type=GuaranteeType.EXACTLY_ONCE,
                count=1,
                docker_args=["--rps", "5"]
            )
        )
    )
    print(scenario)



if __name__ == "__main__":
    asyncio.run(main())



