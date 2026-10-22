import abc
from typing import TypeVar, Generic

ConfigT = TypeVar("ConfigT")

class BaseCreator(abc.ABC, Generic[ConfigT]):
    def __init__(self):
        pass

    @abc.abstractmethod
    def configurate(self, config: ConfigT):
        pass

