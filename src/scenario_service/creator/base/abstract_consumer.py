from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.creator.base.abstract_service import BaseServiceCreator
from src.scenario_service.dto.consumer_config import ConsumerConfig


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
            *args,
            **kwargs
    ):
        return await super().configurate(scenario_id, config, *args, **kwargs)

