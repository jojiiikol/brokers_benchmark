from src.docker_configurator import ContainerConfig
from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.creator.base.abstract_broker import BaseBrokerCreator
from src.scenario_service.dto.broker_config import BrokerConfig


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
                name=f"rabbitmq-{scenario_id}",
                network_name=f"network-{scenario_id}",
                labels={"component": "broker"},
            ),
            extra_labels={"scenario_id": scenario_id},
        )
        return container
