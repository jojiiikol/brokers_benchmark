from src.scenario_service.creator.producer.producer_creator import ProducerCreator
from src.scenario_service.dto.broker_config import Brokers
from src.scenario_service.dto.producer_config import ProducerConfig


class RabbitMQProducerCreator(ProducerCreator):
    """Create producer containers that use the RabbitMQ component image."""

    def component_broker_type(self, config: ProducerConfig) -> str:
        return Brokers.RABBITMQ.value
