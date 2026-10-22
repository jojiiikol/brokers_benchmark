import asyncio
import dataclasses
import datetime
import enum
import logging
import os
import pathlib

from src import setup_logging
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

    async def create_scenario(
            self,
            scenario_config: ScenarioConfig,
    ) -> Scenario:

        scenario = Scenario(
            id="1",
            name="scenario 1",
            containers=[],
            config=scenario_config,
            finished_at=None,
            status=ScenarioStatus.CREATED,
            created_at=datetime.datetime.now(),
        )

        self.configurator.broker_configure(scenario.config.broker_config)

        network = await self.docker_service.create_network(
            name="scenario-5",
        )

        producer_image_tag = await self.docker_service.get_image(
            path=COMPONENTS_PATH / "consumers" / "rabbit_mq",
            tag="producer-rabbitmq",
            path_dockerfile="Dockerfile",
            labels={"component": "producer"},
        )
        producer_container = await self.docker_service.create_container(
            container_config=ContainerConfig(
                image=producer_image_tag,
                name="producer-8",
                network_name="scenario-1"
            )
        )

        broker_image = await self.docker_service.get_image(
            tag="rabbitmq"
        )
        broker_container = await self.docker_service.create_container(
            container_config=ContainerConfig(
                image=broker_image,
                name="rabbitmq-8",
                network_name="scenario-1"
            )
        )

        await network.connect({"Container": broker_container})
        await network.connect({"Container": producer_container})

        scenario.containers.append(producer_container)
        scenario.containers.append(broker_container)

        logger.info(f"Created scenario {scenario.id}")
        return scenario


    async def start_scenario(self, scenario: Scenario) -> str | None:
        scenario.status = ScenarioStatus.RUNNING
        return scenario.id

    # async def get_scenarios_to_finish(self):
    #     running_scenarios = list(filter(lambda scenario: scenario.status == ScenarioStatus.RUNNING, self.scenario_storage))
    #     scenarios_to_finish = []
    #
    #     for scenario in running_scenarios:
    #         current_datetime = datetime.datetime.now()
    #         if current_datetime - scenario.created_at >= datetime.timedelta(seconds=self.TIMEOUT_SCENARIO_SECONDS):
    #             scenarios_to_finish.append(scenario)
    #
    #     logger.info(f"Scenarios: {scenarios_to_finish} ready to finish")
    #     return scenarios_to_finish

    async def stop_scenario(self, scenario: Scenario) -> None:
        scenario.status = ScenarioStatus.STOPPED
        scenario.finished_at = datetime.datetime.now()

        logger.info(f"Scenario: {scenario.id} stopped")

    async def get_status(self):
        msg = []
        for scenario in self.scenario_storage:
            scenario_text = "Scenario %s: status - %s" % (scenario.id, scenario.status)
            msg.append(scenario_text)

        return "\n".join(msg)




    # async def background_task(
    #         self,
    # ):
    #     while True:
    #         await self.start_scenario()
    #         timeout_scenario = await self.get_scenarios_to_finish()
    #
    #         for scenario in timeout_scenario:
    #             await self.stop_scenario(scenario)
    #
    #         await asyncio.sleep(3)
    #         print(await self.get_status())
    #
    # async def start_background(self):
    #     task = asyncio.create_task(self.background_task())
    #     await asyncio.gather(task, return_exceptions=True)


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
                count=1
            )
        )
    )
    print(scenario)



if __name__ == "__main__":
    asyncio.run(main())



