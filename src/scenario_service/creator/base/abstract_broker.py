import abc
from typing import Generic

from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.creator.base.abstract_creator import BaseCreator, ConfigT
from src.scenario_service.dto.interaction_mode import InteractionMode


class BaseBrokerCreator(BaseCreator[ConfigT], Generic[ConfigT]):
    def __init__(
            self,
            docker_service: SdkDockerService,
    ):
        self.docker_service = docker_service

    async def configurate(
            self,
            scenario_id: str,
            config: ConfigT,
            *args,
            **kwargs
    ):
        pass

    def interaction_mode_args(self, mode: InteractionMode) -> tuple[str, ...]:
        raise NotImplementedError

    def consumer_mode_args(self) -> tuple[str, ...]:
        return ()
