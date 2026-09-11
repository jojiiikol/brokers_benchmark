from base import BaseDockerService
import docker

class SdkDockerService(BaseDockerService):
    def __init__(self, **kwargs):
        self.client = docker.from_env()

    async def create_container(
            self,
            image: str,
            name: str,
            **kwargs
    ):
        pass
