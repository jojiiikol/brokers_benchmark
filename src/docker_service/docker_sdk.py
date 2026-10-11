import asyncio
import logging

from aiodocker.containers import DockerContainer

from src.docker_configurator import ContainerConfig, build_docker_payload, PortBinding
from .base import BaseDockerService
import aiodocker

logger = logging.getLogger("DockerService")

class SdkDockerService():
    def __init__(self, **kwargs):
        self.client = aiodocker.Docker()

    async def close_client(self):
        await self.client.close()

    async def create_container(
            self,
            container_config: ContainerConfig,
    ) -> str | None:
        try:
            await self.client.images.pull("rabbitmq", tag="management")

            config, name, _ = build_docker_payload(container_config)
            if not config:
                logger.warning("Config is not valid!")
                return None

            container = await self.client.containers.create(
                config=config,
                name=name,
            )

            return container.id
        except Exception as e:
            logger.error(e)
            return None

    async def start_container(
            self,
            container: DockerContainer,
    ):
        await self.client.containers.start(
            container,
        )

    async def get_list_containers(
            self,
    ) -> list[DockerContainer]:
        containers = await self.client.containers.list(
            all=True,
        )

        return containers

    async def get_container(
            self,
            container_id: str,
    ) -> DockerContainer | None:
        try:
            container = await self.client.containers.get(
                container_id
            )
        except Exception as e:
            logger.warning("Container not found!")
            return None

        return container

    async def stop_container(
            self,
            container_id: str,
    ):
        container = await self.get_container(
            container_id=container_id,
        )
        if container is None:
            return None

        await container.stop()
        await container.delete()


async def main():
    service = SdkDockerService()

    container_id = await service.create_container(
        ContainerConfig(
            image="rabbitmq:management",
            name="rabbit2",
            ports=[PortBinding(
                host_port=15567,
                container_port=15567,
            )],
        )
    )
    container = await service.get_container(
        container_id=container_id,
    )
    await container.start()
    await service.close_client()

if __name__ == '__main__':
    asyncio.run(main())
