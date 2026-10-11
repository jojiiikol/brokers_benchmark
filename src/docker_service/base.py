from abc import ABC, abstractmethod

from src.docker_configurator import ContainerConfig


class BaseDockerService(ABC):

    @abstractmethod
    async def create_container(
            self,
            container_config: ContainerConfig,
    ) -> str | None:
        pass

    @abstractmethod
    async def start_container(self):
        pass

    @abstractmethod
    async def stop_container(self):
        pass


