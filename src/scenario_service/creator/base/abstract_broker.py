import abc
from typing import Generic

from src.scenario_service.creator.base.abstract_creator import BaseCreator, ConfigT
from src.scenario_service.dto.config import Brokers


class BaseBrokerCreator(BaseCreator[ConfigT], Generic[ConfigT]):
    def __init__(
            self,
            broker_type: Brokers
    ):
        self.broker_type = broker_type

    def configurate(
            self,
            config: ConfigT
    ):
        pass
