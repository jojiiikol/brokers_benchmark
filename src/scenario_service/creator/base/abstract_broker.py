import abc
from typing import Generic

from src.scenario_service.creator.base.abstract_creator import BaseCreator, ConfigT
from src.scenario_service.dto.broker_config import Brokers


class BaseBrokerCreator(BaseCreator[ConfigT], Generic[ConfigT]):
    def __init__(
            self,
    ):
        pass

    def configurate(
            self,
            config: ConfigT
    ):
        pass
