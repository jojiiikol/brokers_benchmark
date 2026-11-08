import abc
import pathlib
from typing import TypeVar, Generic

ConfigT = TypeVar("ConfigT")

class BaseCreator(abc.ABC, Generic[ConfigT]):
    COMPONENTS_PATH = pathlib.Path(__file__).resolve().parent.parent.parent.parent / "components"

    @abc.abstractmethod
    async def configurate(self, scenario_id: str, config: ConfigT, *args, **kwargs):
        pass

