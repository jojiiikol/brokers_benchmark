import asyncio
import dataclasses
import datetime
import enum
import logging
import os
import pathlib
import uuid

from src.docker_configurator import ContainerConfig
from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.configurator import ComponentConfigurator
from src.scenario_service.dto.broker_config import BrokerConfig, Brokers
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.producer_config import ProducerConfig, LoadType, GuaranteeType
from src.scenario_service.dto.scenario_config import ScenarioConfig

logger = logging.getLogger("my_app")


COMPONENTS_PATH = pathlib.Path(__file__).resolve().parent.parent / "components"

class ScenarioStatus(enum.Enum):
    CREATED = 'CREATED'
    PENDING = 'PENDING'
    INITIALIZING = 'INITIALIZING'
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
        self.configurator = ComponentConfigurator(docker_service)

    async def initialize_scenario(
            self,
            scenario: Scenario,
    ) -> Scenario:
        scenario_id = scenario.id
        scenario.status = ScenarioStatus.INITIALIZING
        try:
            self.configurator.broker_configure(scenario.config.broker_config)

            network = await self.docker_service.create_network(
                name=f"network-{scenario_id}",
            )

            broker_image = await self.docker_service.get_image(
                tag="rabbitmq"
            )

            broker_container = await self.docker_service.create_container(
                container_config=ContainerConfig(
                    image=broker_image,
                    name=f"rabbitmq-{scenario_id}",
                    network_name=f"network-{scenario_id}",
                )
            )

            scenario.config.producer_config.docker_args.extend([
                "--broker-host", f"rabbitmq-{scenario_id}",
                "--broker-port", "5672",
            ])

            producer_image_tag = await self.docker_service.get_image(
                path=COMPONENTS_PATH / "consumers" / "rabbit_mq",
                tag="producer-rabbitmq",
                path_dockerfile="Dockerfile",
                labels={"component": "producer"},
            )
            producer_container = await self.docker_service.create_container(
                container_config=ContainerConfig(
                    image=producer_image_tag,
                    name=f"producer-{scenario_id}",
                    network_name=f"network-{scenario_id}",
                    cmd=scenario.config.producer_config.docker_args,
                )
            )

            await network.connect({"Container": broker_container})
            await network.connect({"Container": producer_container})

            scenario.containers.append(producer_container)
            scenario.containers.append(broker_container)

            scenario.status = ScenarioStatus.PENDING
        except Exception as e:
            logger.error(e)
            scenario.status = ScenarioStatus.ERROR

        return scenario


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
        scenario.status = ScenarioStatus.RUNNING
        return scenario.id




async def main():
    service = ScenarioService()
    await service.docker_service.open_client()
    scenario = await service.create_scenario(
        scenario_config=ScenarioConfig(
            broker_config=BrokerConfig(
                broker_type=Brokers.RABBITMQ
            ),
            consumer_config=ConsumerConfig(
                count=1
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



