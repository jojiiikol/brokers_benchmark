import abc

from src.docker_configurator import ContainerConfig
from src.docker_configurator.schema import ResourceLimits
from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.creator.base.abstract_creator import BaseCreator, ConfigT
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.broker_config import Brokers
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
        self._image_tags: dict[tuple[str, str], str] = {}

    async def configurate(
            self,
            scenario_id: str,
            config: ConfigT,
            broker_type: Brokers,
            docker_args: list[str],
            instance_index: int | None = None,
            *args,
            **kwargs
    ):
        components = {
            ProducerConfig: "producer",
            ConsumerConfig: "consumer",
        }

        component_type = components.get(type(config), None)
        if component_type is None:
            raise ValueError("Config type not supported")

        component_broker_type = self.component_broker_type(broker_type)
        image_key = (component_type, component_broker_type)
        image_tag = self._image_tags.get(image_key)
        if image_tag is None:
            image_tag = await self.docker_service.get_image(
                path=self.SOURCE_PATH,
                tag=f"{component_type}-{component_broker_type}",
                path_dockerfile=(
                    f"components/{component_type}/{component_broker_type}/Dockerfile"
                ),
                labels={"component": component_type},
            )
            self._image_tags[image_key] = image_tag

        instance_suffix = "" if instance_index is None else f"-{instance_index + 1}"
        container = await self.docker_service.create_container(
            container_config=ContainerConfig(
                image=image_tag,
                name=f"{component_type}-{scenario_id}{instance_suffix}",
                network_name=f"network-{scenario_id}",
                cmd=docker_args,
                resources=ResourceLimits(
                    nano_cpus=round(config.cpu_limit * 1_000_000_000),
                    memory=(
                        config.memory_limit_mib * 1024 ** 2
                        if config.memory_limit_mib is not None
                        else None
                    ),
                ),
                labels={
                    "component": component_type,
                    "prometheus.scrape": "true",
                },
            ),
            extra_labels={"scenario_id": scenario_id},
        )
        return container

    def component_broker_type(self, broker_type: Brokers) -> str:
        return broker_type.value
