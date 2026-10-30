from src.scenario_service.creator.base.abstract_producer import BaseProducerCreator
from src.scenario_service.dto.producer_config import ProducerConfig


class ProducerCreator(BaseProducerCreator):
    def __init__(
            self,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        pass

    async def configurate(
            self,
            scenario_id: str,
            config: ProducerConfig,
            *args,
            **kwargs
    ):
        return await super().configurate(scenario_id, config, *args, **kwargs)