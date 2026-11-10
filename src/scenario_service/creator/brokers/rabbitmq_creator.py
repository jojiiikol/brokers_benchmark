from src.docker_configurator import ContainerConfig
from src.docker_configurator.schema import ResourceLimits
from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.creator.base.abstract_broker import BaseBrokerCreator
from src.scenario_service.dto.broker_config import BrokerConfig
from src.scenario_service.dto.interaction_mode import InteractionMode

class RabbitMQCreator(BaseBrokerCreator):
    def __init__(
            self,
            docker_service: SdkDockerService,
    ):
        super().__init__(docker_service)

    async def configurate(
            self,
            scenario_id: str,
            config: BrokerConfig,
            *args,
            **kwargs
    ):
        image = await self.docker_service.get_image(
            tag="rabbitmq"
        )
        container = await self.docker_service.create_container(
            container_config=ContainerConfig(
                image=image,
                name=self.broker_hostname(scenario_id),
                network_name=f"network-{scenario_id}",
                labels={"component": "broker"},
                resources=ResourceLimits(
                    memory=1 * 1024 * 1024 * 1024,
                    nano_cpus=1 * 1_000_000_000
                )
            ),
            extra_labels={"scenario_id": scenario_id},
        )
        return container

    def broker_hostname(self, scenario_id: str) -> str:
        return f"rabbitmq-{scenario_id}"

    def interaction_mode_args(self, mode: InteractionMode) -> tuple[str, ...]:
        if mode not in (InteractionMode.WORK_QUEUE, InteractionMode.PUB_SUB):
            raise ValueError(f"RabbitMQ does not support interaction mode {mode.value}")
        return ("--interaction-mode", mode.value)
