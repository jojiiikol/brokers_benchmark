import dataclasses

from src.scenario_service.creator.consumer.consumer_creator import ConsumerCreator
from src.scenario_service.creator.base.abstract_consumer import BaseConsumerCreator
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.interaction_mode import InteractionMode


class KafkaConsumerCreator(ConsumerCreator):
    """Assign Kafka consumer groups according to the interaction topology."""

    async def configurate(
            self,
            scenario_id: str,
            config: ConsumerConfig,
            interaction_mode: InteractionMode = InteractionMode.WORK_QUEUE,
            *args,
            **kwargs,
    ) -> list[str | None]:
        if config.count <= 0:
            raise ValueError("Consumer count must be greater than zero")

        containers = []
        for index in range(config.count):
            group_id = "scenario-workers"
            if interaction_mode is InteractionMode.PUB_SUB:
                group_id = f"scenario-subscriber-{index + 1}"
            instance_config = dataclasses.replace(
                config,
                docker_args=[*config.docker_args, "--group-id", group_id],
            )
            container = await BaseConsumerCreator.configurate(
                self,
                scenario_id,
                instance_config,
                instance_index=index,
                *args,
                **kwargs,
            )
            containers.append(container)
        return containers
