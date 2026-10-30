import abc

from src.docker_configurator import ContainerConfig
from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.creator.base.abstract_creator import BaseCreator, ConfigT
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.producer_config import ProducerConfig


class BaseServiceCreator(BaseCreator):
    def __init__(
            self,
            docker_service: SdkDockerService,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.docker_service = docker_service

    async def configurate(
            self,
            scenario_id: str,
            config: ConfigT,
            *args,
            **kwargs
    ):
        await super().configurate(scenario_id, config, *args, **kwargs)
        components = {
            ProducerConfig: "producer",
            ConsumerConfig: "consumer",
        }

        component_type = components.get(type(config), None)
        if component_type is None:
            raise ValueError("Config type not supported")

        image_tag = await self.docker_service.get_image(
            path=self.COMPONENTS_PATH / component_type / config.broker_connection.broker_type.value,
            tag=f"{component_type}-{config.broker_connection.broker_type.value}",
            path_dockerfile="Dockerfile",
            labels={"component": component_type},
        )
        container = await self.docker_service.create_container(
            container_config=ContainerConfig(
                image=image_tag,
                name=f"{component_type}-{scenario_id}",
                network_name=f"network-{scenario_id}",
                cmd=config.docker_args,
            ),
            extra_labels={"scenario_id": scenario_id},
        )
        return container