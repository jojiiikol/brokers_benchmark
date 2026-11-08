
from src.scenario_service.creator.base.abstract_consumer import BaseConsumerCreator
from src.scenario_service.dto.consumer_config import ConsumerConfig


class ConsumerCreator(BaseConsumerCreator):
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
            config: ConsumerConfig,
            *args,
            **kwargs
    ):
        if config.count <= 0:
            raise ValueError("Consumer count must be greater than zero")

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
