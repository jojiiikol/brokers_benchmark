import asyncio
import io
import logging
import pathlib
import tarfile
import time
from typing import AsyncGenerator

from aiodocker.containers import DockerContainer
from aiodocker.networks import DockerNetwork

from src import setup_logging
from src.config import CONTAINERS_NAMESPACE
from src.docker_configurator import ContainerConfig, build_docker_payload
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

    async def create_container(
            self,
            container_config: ContainerConfig,
            extra_labels: dict[str, str] | None = None,
    ) -> str | None:
        try:
            container_config.labels.update(
                {
                    "namespace": CONTAINERS_NAMESPACE,
                },
                **extra_labels if extra_labels is not None else {},
            )

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
            container_id: str,
    ):
        container = await self.get_container(
            container_id=container_id,
        )

        await container.start()
        if await self.wait_container_ready(container.id):
            logger.info(f"Container {container.id} started")
            return

        logger.error(f"Container {container.id} not started successfully")

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

    async def _get_resources_by_labels(
            self,
            resource_collection,
            labels: dict[str, str],
            all: bool = False,
    ) -> list:

        filters = {
            "label": [
                f"{key}={value}"
                for key, value in labels.items()
            ]
        }
        if all:
            return await resource_collection.list(
                filters=filters,
                all=True,
            )

        return await resource_collection.list(
            filters=filters,
        )

    async def stop_container(
            self,
            container_id: str,
    ):
        container = await self.get_container(
            container_id=container_id,
        )
        if container is None:
            return

        await container.stop()
        await container.delete()

    async def clear_resources(self):
        containers = await self._get_resources_by_labels(
            resource_collection=self.client.containers,
            labels={"namespace": CONTAINERS_NAMESPACE},
            all=True,
        )

        networks = await self._get_resources_by_labels(
            resource_collection=self.client.networks,
            labels={"namespace": CONTAINERS_NAMESPACE},
        )

        # Удаляем контейнеры
        for container in containers:
            try:
                await container.delete(force=True)
                logger.info("Container %s deleted", container.id)
            except Exception:
                logger.exception(
                    "Failed to delete container %s",
                    container.id,
                )

        # Удаляем сети
        for network in networks:
            network_id = network["Id"]

            try:
                network_object = await self.client.networks.get(network_id)
                await network_object.delete()

                logger.info("Network %s deleted", network_id)
            except Exception:
                logger.exception(
                    "Failed to delete network %s",
                    network_id,
                )

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
        stream = self.client.images.pull(tag, stream=True)
        await self._image_logs(stream)
        return tag

    async def build_image(
            self,
            path: pathlib.Path,
            tag: str,
            path_dockerfile: str,
            labels: dict[str, str] | None = None,
    ) -> str | None:
        # Надо понять как строить путь
        # Генерция тега
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

    async def get_container_state(
            self,
            container: DockerContainer,
    ) -> dict:
        info = await container.show()

        state = info.get("State", {})

        return {
            "status": state.get("Status"),
            "running": state.get("Running"),
            "exit_code": state.get("ExitCode"),
        }

    async def wait_container_ready(
            self,
            container_id: str,
            timeout: float = 60,
            interval: float = 2,
    ) -> bool:
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            container = await self.get_container(container_id)

            if container is None:
                return False

            state = await self.get_container_state(container)

            if state["status"] in ("exited", "dead"):
                return False

            if state["running"]:
                return True

            await asyncio.sleep(interval)

        return False




async def main():
    service = SdkDockerService()
    print(pathlib.Path(__file__).resolve().parent.parent / "components" / "producer" / "rabbit_mq")
    await service.open_client()
    a = await service.build_image(
        path=pathlib.Path(__file__).resolve().parent.parent / "components" / "producer" / "rabbit_mq",
        tag="rabbit_mq-producer",
        path_dockerfile="Dockerfile",
        labels=None
    )
    print(a)
    await service.client.close()

if __name__ == '__main__':
    asyncio.run(main())
