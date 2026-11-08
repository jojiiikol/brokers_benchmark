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
        if config.count <= 0:
            raise ValueError("Producer count must be greater than zero")

        containers = []
        for index in range(config.count):
            container = await super().configurate(
                scenario_id,
                config,
                instance_index=index,
                *args,
                **kwargs,
            )
            containers.append(container)
        return containers
