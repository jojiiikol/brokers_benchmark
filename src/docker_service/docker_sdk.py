import asyncio
import io
import logging
import pathlib
import tarfile
from itertools import starmap

from aiodocker.containers import DockerContainer

from src.docker_configurator import ContainerConfig, build_docker_payload, PortBinding
from .base import BaseDockerService
import aiodocker

logger = logging.getLogger("DockerService")

class SdkDockerService():
    BASE_DIR = pathlib.Path(__file__).resolve().parent
    COMPONENT_PATH = BASE_DIR / "components"

    def __init__(self, **kwargs):
        self.client = aiodocker.Docker()

    async def close_client(self):
        await self.client.close()

    async def create_container(
            self,
            container_config: ContainerConfig,
    ) -> str | None:
        try:
            await self.client.images.pull("rabbit_mq", tag="management")

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

    def _create_build_context(self, context_path: pathlib.Path) -> io.BytesIO:
        archive = io.BytesIO()

        with tarfile.open(fileobj=archive, mode="w") as tar:
            for path in context_path.rglob("*"):
                if path.is_file():
                    tar.add(
                        path,
                        arcname=path.relative_to(context_path),
                    )

        archive.seek(0)
        return archive

    async def build_image(
            self,
            path: pathlib.Path,
            tag: str,
            path_dockerfile: str,
            labels: dict[str, str] | None = None,
    ) -> str | None:
        # Надо понять как строить путь
        # Генерция тега
        try:
            archive = self._create_build_context(path)
            await self.client.images.build(
                fileobj=archive,
                path_dockerfile=path_dockerfile,
                encoding="identity",
                tag=tag,
                labels=labels
            )
            return tag
        except Exception as e:
            logger.error(e)
            return None




async def main():
    service = SdkDockerService()
    a = await service.build_image(
        path=pathlib.Path(__file__).resolve().parent / "components" / "consumers" / "rabbit_mq",
        tag="rabbit_mq-consumers",
        path_dockerfile="Dockerfile",
        labels=None
    )
    print(a)
    await service.client.close()

if __name__ == '__main__':
    asyncio.run(main())
