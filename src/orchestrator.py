import asyncio
import logging

from src import setup_logging
from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.dto.broker_config import BrokerConfig, Brokers
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.producer_config import ProducerConfig, GuaranteeType, LoadType
from src.scenario_service.service import ScenarioService, ScenarioConfig, Scenario, ScenarioStatus

setup_logging()

logger = logging.getLogger(__name__)

class Orchestrator:
    MAX_RUNNING_SCENARIO_COUNT: int = 5

    def __init__(self):
        self.docker_service = SdkDockerService()
        self.scenario_service = ScenarioService(self.docker_service)
        self.scenario_storage: list = []

        self.create_image_lock = asyncio.Lock()

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

    def get_info(self):
        full_text = []
        for key, scenario in enumerate(self.scenario_storage):
            full_text.append(
                f"Number {key} | id: {scenario.id} | status: {scenario.status}"
            )
        return "\n".join(full_text)

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

    async def _initialize_scenario(self):
        async with self.create_image_lock:
            older = self._get_older_scenario
            if older is None:
                logger.info("Not new scenario")
                return

            await self.scenario_service.initialize_scenario(older)

    async def _process(self) -> None:
        while True:
            logger.info(self.get_info())
            await self._initialize_scenario()

            await asyncio.sleep(3)

    async def run(self):
        task = asyncio.create_task(self._process())
        await asyncio.gather(task, return_exceptions=True)



async def main():
    orchestrator = Orchestrator()
    await orchestrator.docker_service.open_client()
    await orchestrator.create_scenario(
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


    await orchestrator.run()
    await orchestrator.docker_service.client.close()


if __name__ == "__main__":
    asyncio.run(main())
