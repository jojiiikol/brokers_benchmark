import dataclasses

from src.docker_service.docker_sdk import SdkDockerService


class ComponentConfigurator:
    def __init__(
            self,
            docker_service: SdkDockerService = SdkDockerService()
    ):
        self.docker_service = docker_service


    def create_consumer(
            self,
            message_size: int,
            speed: int,
    ):
        pass

