import asyncio
import io
import logging
import pathlib
import tarfile
from itertools import starmap
from typing import AsyncGenerator

from aiodocker.containers import DockerContainer
from aiodocker.networks import DockerNetwork

from src import setup_logging
from src.docker_configurator import ContainerConfig, build_docker_payload, PortBinding
from .base import BaseDockerService
import aiodocker
setup_logging()
logger = logging.getLogger("DockerService")

class SdkDockerService:
    BASE_DIR = pathlib.Path(__file__).resolve().parent
    COMPONENT_PATH = BASE_DIR / "components"

    def __init__(self, **kwargs):
        self.client = None

    async def open_client(self):
        self.client = aiodocker.Docker()

    async def close_client(self):
        await self.client.close()

    async def create_network(
            self,
            name: str,
            labels: dict[str, str] | None = None,
    ) -> DockerNetwork | None:
        try:
            network = await self.client.networks.create(
                config={
                    "Name": name,
                    "Driver": "bridge",
                    "CheckDuplicate": True,
                    "Internal": False,
                    "Attachable": True,
                    "Labels": labels
                }
            )
            logger.info(f"Created network: {name}")
            return network
        except Exception as e:
            logger.error(e)
            return None



    async def create_container(
            self,
            container_config: ContainerConfig,
    ) -> str | None:
        try:
            config, name, _ = build_docker_payload(container_config)
            if not config:
                logger.warning("Config is not valid!")
                return None

            container = await self.client.containers.create(
                config=config,
                name=name,
            )

            logger.info(f"Created container: {name}")
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

    async def _image_logs(
            self,
            stream: AsyncGenerator[bytes, None],
    ):
        async for chunk in stream:
            if "stream" in chunk:
                logger.info(chunk["stream"].strip())
            if "error" in chunk:
                logger.error(f"Build error: {chunk['error']}")
                return None

    async def get_image(
            self,
            tag: str,
            path_dockerfile: str | None = None,
            path: pathlib.Path | None = None,
            labels: dict[str, str] | None = None,
    ) -> str | None:
        image = None
        if tag is None:
            logger.error("No tag provided")
            return None

        if path is None:
            image =  await self.pull_image(tag)
        else:
            image =  await self.build_image(
                path=path,
                tag=tag,
                path_dockerfile=path_dockerfile,
                labels=labels,
            )

        logger.info(f"Created image: {image}")
        return image

    async def pull_image(
            self,
            tag: str,
    ):
        try:
            stream = self.client.images.pull(tag, stream=True)
            await self._image_logs(stream)
            return tag
        except Exception as e:
            logger.error(e)
            return None

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
            build_stream = self.client.images.build(
                fileobj=archive,
                path_dockerfile=path_dockerfile,
                encoding="identity",
                tag=tag,
                labels=labels,
                stream=True,
            )
            await self._image_logs(build_stream)
            return tag

        except Exception as e:
            logger.error(e)
            return None




async def main():
    service = SdkDockerService()
    print(pathlib.Path(__file__).resolve().parent.parent / "components" / "consumers" / "rabbit_mq")
    await service.open_client()
    a = await service.build_image(
        path=pathlib.Path(__file__).resolve().parent.parent / "components" / "consumers" / "rabbit_mq",
        tag="rabbit_mq-consumers",
        path_dockerfile="Dockerfile",
        labels=None
    )
    print(a)
    await service.client.close()

if __name__ == '__main__':
    asyncio.run(main())
