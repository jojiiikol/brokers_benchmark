from src.scenario_service.creator.consumer.consumer_creator import ConsumerCreator
from src.scenario_service.dto.broker_config import Brokers
from src.scenario_service.dto.consumer_config import ConsumerConfig


class RabbitMQConsumerCreator(ConsumerCreator):
    """Create consumer containers that use the RabbitMQ component image."""

    def component_broker_type(self, broker_type: Brokers) -> str:
        return Brokers.RABBITMQ.value
