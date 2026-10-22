import abc
from typing import Generic

from src.scenario_service.creator.base.abstract_broker import BaseBrokerCreator
from src.scenario_service.creator.base.abstract_creator import BaseCreator, ConfigT
from src.scenario_service.creator.dto.broker_config import RabbitMQConfig
from src.scenario_service.dto.config import Brokers


class RabbitMQCreator(BaseBrokerCreator):
    def __init__(
            self,
            broker_type: Brokers
    ):
        self.broker_type = broker_type

    def configurate(
            self,
            config: RabbitMQConfig
    ):
        pass
