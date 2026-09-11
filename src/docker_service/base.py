from abc import ABC, abstractmethod


class BaseDockerService(ABC):

    @abstractmethod
    async def create_container(
            self,
            image: str,
            name: str,
            **kwargs
    ):
        pass

    @abstractmethod
    async def start_container(self):
        pass

    @abstractmethod
    async def stop_container(self):
        pass


