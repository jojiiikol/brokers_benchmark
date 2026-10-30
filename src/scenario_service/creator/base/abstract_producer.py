
from src.scenario_service.creator.base.abstract_service import BaseServiceCreator
from src.scenario_service.dto.producer_config import ProducerConfig


class BaseProducerCreator(BaseServiceCreator):
    async def configurate(
            self,
            scenario_id: str,
            config: ProducerConfig,
            *args,
            **kwargs
    ):
        return await super().configurate(scenario_id, config, *args, **kwargs)
