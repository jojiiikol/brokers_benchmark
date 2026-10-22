import asyncio
import dataclasses
import datetime
import enum
import logging

from src import setup_logging
from src.docker_configurator import ContainerConfig
from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.dto.config import ScenarioConfig

logger = logging.getLogger("my_app")



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
            docker_service: SdkDockerService
    ):
        self.docker_service = docker_service

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


        if scenario.config.broker_type == "scenario_1":
            container_id = await self.docker_service.create_container(
                container_config=ContainerConfig(
                    image="scenario",
                    name="scenario_1",
                )
            )
            scenario.containers.append(container_id)

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
    await service.create_scenario(
        scenario_config={}
    )
    await service.create_scenario(
        scenario_config={}
    )
    await service.create_scenario(
        scenario_config={}
    )
    await service.create_scenario(
        scenario_config={}
    )
    await service.start_background()


if __name__ == "__main__":
    asyncio.run(main())



