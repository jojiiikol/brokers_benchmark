
import dataclasses

from src.scenario_service.creator.base.abstract_service import BaseServiceCreator
from src.scenario_service.dto.producer_config import ProducerConfig


class BaseProducerCreator(BaseServiceCreator):
    async def configurate(
            self,
            scenario_id: str,
            config: ProducerConfig,
            instance_index: int | None = None,
            *args,
            **kwargs
    ):
        docker_args = [
            *config.docker_args,
            "--rps", str(config.rps),
            "--payload_size", str(config.payload_size),
        ]
        container_config = dataclasses.replace(config, docker_args=docker_args)
        return await super().configurate(
            scenario_id,
            container_config,
            instance_index=instance_index,
            *args,
            **kwargs,
        )
