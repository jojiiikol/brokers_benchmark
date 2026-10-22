import asyncio

from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.dto.config import Brokers, LoadType
from src.scenario_service.service import ScenarioService, ScenarioConfig, Scenario, ScenarioStatus


class Orchestrator:
    MAX_RUNNING_SCENARIO_COUNT: int = 5

    def __init__(self):
        self.docker_service = SdkDockerService()
        self.scenario_service = ScenarioService(self.docker_service)
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
            scenario_config: ScenarioConfig
    ) -> str:
        scenario = await self.scenario_service.create_scenario(scenario_config)
        self.scenario_storage.append(scenario)
        return scenario.id

    def start_scenario(self):
        if not self._check_run_available:
            return

        older_scenario = self._get_older_scenario
        if older_scenario is None:
            return

        scenario_id = self.scenario_service.start_scenario(
            scenario=older_scenario,
        )
        return scenario_id


async def main():
    orchestrator = Orchestrator()
    scenario_config = ScenarioConfig(
        broker_type=Brokers.RABBITMQ,
        consumers_count=1,
        producers_count=1,
        load_type=LoadType.linear,
        guarantee_type="1",
        intensity=100,
        message_size=8
    )
    scenario_config_2 = ScenarioConfig(
        broker_type=Brokers.KAFKA,
        consumers_count=1,
        producers_count=2,
        load_type=LoadType.exponential,
        guarantee_type="3",
        intensity=250,
        message_size=64
    )

    await orchestrator.create_scenario(scenario_config)
    await orchestrator.create_scenario(scenario_config_2)
    print(orchestrator.scenario_storage)
    await orchestrator.start_scenario()
    print(orchestrator.scenario_storage)


if __name__ == "__main__":
    asyncio.run(main())
