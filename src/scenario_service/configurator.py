import dataclasses

from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.creator.base.abstract_broker import BaseBrokerCreator
from src.scenario_service.creator.base.abstract_creator import ConfigT
from src.scenario_service.creator.brokers.rabbitmq_creator import RabbitMQCreator
from src.scenario_service.creator.consumer.consumer_creator import ConsumerCreator
from src.scenario_service.creator.producer.producer_creator import ProducerCreator
from src.scenario_service.dto.broker_config import BrokerConfig
from src.scenario_service.dto.consumer_config import ConsumerConfig


class ComponentConfigurator:
    def __init__(
            self,
            docker_service: SdkDockerService,
            consumer_creator: ConsumerCreator = ConsumerCreator(),
            broker_creator: BaseBrokerCreator[BrokerConfig] = RabbitMQCreator(),
            producer_creator: ProducerCreator = ProducerCreator(),
    ):
        self.docker_service = docker_service
        self.consumer_creator = consumer_creator
        self.broker_configurator = broker_creator
        self.producer_configurator = producer_creator


    def broker_configure(
            self,
            config: BrokerConfig,
    ):
        self.broker_configurator.configurate(config)

