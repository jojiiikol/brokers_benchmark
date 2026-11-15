from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.creator.base.abstract_service import BaseServiceCreator
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.broker_config import Brokers


class BaseConsumerCreator(BaseServiceCreator):
    def __init__(
            self,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)

    async def configurate(
            self,
            scenario_id: str,
            config: ConsumerConfig,
            broker_type: Brokers,
            docker_args: list[str],
            instance_index: int | None = None,
            *args,
            **kwargs
    ):
        return await super().configurate(
            scenario_id,
            config,
            broker_type=broker_type,
            docker_args=docker_args,
            instance_index=instance_index,
            *args,
            **kwargs,
        )

