import asyncio
import dataclasses
import datetime
import enum
import logging

from src import setup_logging

logger = logging.getLogger("my_app")

class ScenarioConfig:
    broker_type: str

    consumers_count: int
    producers_count: int

    load_type: str
    guarantee_type: str
    intensity: str

    message_size: int

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
    config: dict | None
    finished_at: datetime.datetime | None

    status: ScenarioStatus = ScenarioStatus.CREATED
    created_at: datetime.datetime = datetime.datetime.now()

class ScenarioService:
    MAX_RUNNING_SCENARIO_COUNT: int = 5
    TIMEOUT_SCENARIO_SECONDS: int = 45

    def __init__(
            self,
    ):
        self.scenario_storage: list = []

    @property
    def scenario_running_count(self) -> int:
        running_scenarios = list(
            filter(
                lambda scenario: scenario.status == ScenarioStatus.RUNNING,
                self.scenario_storage
            )
        )
        return len(running_scenarios)

    @property
    def _get_older_scenario(self) -> Scenario:
        running_scenarios = list(
            filter(
                lambda scenario: scenario.status == ScenarioStatus.CREATED,
                self.scenario_storage
            )
        )
        older_scenario = min(running_scenarios, key=lambda scenario: scenario.created_at, default=None)
        return older_scenario

    @property
    def _check_run_available(self) -> bool:
        return self.scenario_running_count < ScenarioService.MAX_RUNNING_SCENARIO_COUNT

    async def create_scenario(
            self,
            scenario_config: dict,
    ) -> str | None:
        # id генератор

        scenario = Scenario(
            id=str(len(self.scenario_storage) + 1),
            name="scenario 1",
            containers=[],
            config=scenario_config,
            finished_at=None,
            status=ScenarioStatus.CREATED,
            created_at=datetime.datetime.now(),
        )
        self.scenario_storage.append(scenario)
        logger.info(f"Created scenario {scenario.id}")
        return scenario.id


    async def start_scenario(self) -> str | None:
        if not self._check_run_available:
            return None

        scenario = self._get_older_scenario
        if scenario is None:
            return None

        # обращение в Docker
        scenario.status = ScenarioStatus.RUNNING
        return scenario.id

    async def get_scenarios_to_finish(self):
        running_scenarios = list(filter(lambda scenario: scenario.status == ScenarioStatus.RUNNING, self.scenario_storage))
        scenarios_to_finish = []

        for scenario in running_scenarios:
            current_datetime = datetime.datetime.now()
            if current_datetime - scenario.created_at >= datetime.timedelta(seconds=self.TIMEOUT_SCENARIO_SECONDS):
                scenarios_to_finish.append(scenario)

        logger.info(f"Scenarios: {scenarios_to_finish} ready to finish")
        return scenarios_to_finish

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




    async def background_task(
            self,
    ):
        while True:
            await self.start_scenario()
            timeout_scenario = await self.get_scenarios_to_finish()

            for scenario in timeout_scenario:
                await self.stop_scenario(scenario)

            await asyncio.sleep(3)
            print(await self.get_status())

    async def start_background(self):
        task = asyncio.create_task(self.background_task())
        await asyncio.gather(task, return_exceptions=True)


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



