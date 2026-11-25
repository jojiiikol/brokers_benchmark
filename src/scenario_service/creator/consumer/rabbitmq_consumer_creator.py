from src.scenario_service.creator.consumer.consumer_creator import ConsumerCreator
from src.scenario_service.creator.base.abstract_consumer import BaseConsumerCreator
from src.scenario_service.dto.broker_config import Brokers
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.interaction_mode import InteractionMode


class RabbitMQConsumerCreator(ConsumerCreator):
    """Create consumer containers that use the RabbitMQ component image."""

    SUBSCRIBER_QUEUE_PREFIX = "scenario-subscriber-"

    async def configurate(
            self,
            scenario_id: str,
            config: ConsumerConfig,
            broker_type: Brokers,
            docker_args: list[str],
            interaction_mode: InteractionMode = InteractionMode.WORK_QUEUE,
            *args,
            **kwargs,
    ) -> list[str | None]:
        """Create consumers with stable queues that the drain phase can inspect.

        Every scenario has its own RabbitMQ broker, so the one-based instance
        suffix is unique within the only namespace visible to that broker.
        """
        if config.count <= 0:
            raise ValueError("Consumer count must be greater than zero")

        containers = []
        for index in range(config.count):
            queue_name = "scenario"
            if interaction_mode is InteractionMode.PUB_SUB:
                queue_name = f"{self.SUBSCRIBER_QUEUE_PREFIX}{index + 1}"
            instance_args = [
                *docker_args,
                "--queue-name", queue_name,
                "--exchange-name", "scenario",
            ]
            containers.append(await BaseConsumerCreator.configurate(
                self,
                scenario_id,
                config,
                broker_type=broker_type,
                docker_args=instance_args,
                instance_index=index,
                *args,
                **kwargs,
            ))
        return containers

    def component_broker_type(self, broker_type: Brokers) -> str:
        return Brokers.RABBITMQ.value
