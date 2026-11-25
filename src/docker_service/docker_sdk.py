import asyncio
import io
import json
import logging
import pathlib
import tarfile
import time
from typing import AsyncGenerator

from aiodocker.containers import DockerContainer
from aiodocker.exceptions import DockerError
from aiodocker.networks import DockerNetwork

from src.config import CONTAINERS_NAMESPACE, OBSERVABILITY_NETWORK
from src.docker_configurator import ContainerConfig, build_docker_payload
import aiodocker

logger = logging.getLogger("DockerService")

class SdkDockerService:
    BASE_DIR = pathlib.Path(__file__).resolve().parent
    COMPONENT_PATH = BASE_DIR / "components"

    def __init__(self, **kwargs):
        self.client = None

    async def open_client(self):
        self.client = aiodocker.Docker()
        logger.info("Docker client opened")

    async def close_client(self):
        await self.client.close()
        self.client = None
        logger.info("Docker client closed")

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
        except Exception:
            logger.exception("Failed to create container %s", container_config.name)
            return None

    async def connect_to_observability_network(self, container_id: str) -> None:
        """Attach a dynamic benchmark component to Compose's shared network."""
        network = await self.client.networks.get(OBSERVABILITY_NETWORK)
        await network.connect({"Container": container_id})
        logger.info(
            "Connected container %s to observability network %s",
            container_id,
            OBSERVABILITY_NETWORK,
        )

    async def start_container(
            self,
            container_id: str,
    ) -> bool:
        container = await self.get_container(
            container_id=container_id,
        )
        if container is None:
            return False

        await container.start()
        if await self.wait_container_ready(container.id):
            logger.info(f"Container {container.id} started")
            return True

        logger.error(f"Container {container.id} not started successfully")
        return False

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
        except Exception:
            logger.warning("Container %s not found", container_id)
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
                await container.delete(force=True, v=True)
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

    async def clear_scenario_resources(self, scenario_id: str) -> None:
        """Delete all Docker resources belonging to one scenario.

        Labels make this operation safe to repeat after a partial initialization
        or an interrupted shutdown.
        """
        labels = {
            "namespace": CONTAINERS_NAMESPACE,
            "scenario_id": scenario_id,
        }
        containers = await self._get_resources_by_labels(
            resource_collection=self.client.containers,
            labels=labels,
            all=True,
        )
        for container in containers:
            try:
                await container.delete(force=True, v=True)
                logger.info("Container %s deleted", container.id)
            except Exception:
                logger.exception("Failed to delete container %s", container.id)

        networks = await self._get_resources_by_labels(
            resource_collection=self.client.networks,
            labels=labels,
        )
        for network in networks:
            network_id = network["Id"]
            try:
                network_object = await self.client.networks.get(network_id)
                await network_object.delete()
                logger.info("Network %s deleted", network_id)
            except Exception:
                logger.exception("Failed to delete network %s", network_id)

    async def get_benchmark_container_stats(self) -> list[dict]:
        """Read resource counters for running benchmark containers via Docker API."""
        containers = await self._get_resources_by_labels(
            resource_collection=self.client.containers,
            labels={"namespace": CONTAINERS_NAMESPACE},
        )
        async def collect(container):
            try:
                details = await container.show()
                labels = details.get("Config", {}).get("Labels", {}) or {}
                scenario_id = labels.get("scenario_id")
                component = labels.get("component")
                if not scenario_id or not component:
                    return None

                stats = await container.stats(stream=False)
                # aiodocker returns a one-item list for a non-streaming stats
                # request; the Docker API payload itself is that first item.
                if isinstance(stats, list):
                    stats = stats[0] if stats else {}
                if ("total_usage" not in stats.get("cpu_stats", {}).get("cpu_usage", {})
                        or "usage" not in stats.get("memory_stats", {})):
                    return None
                networks = stats.get("networks", {}) or {}
                network_received = sum(
                    interface.get("rx_bytes", 0) for interface in networks.values()
                )
                network_transmitted = sum(
                    interface.get("tx_bytes", 0) for interface in networks.values()
                )
                disk_written = sum(
                    item.get("value", 0)
                    for item in (
                        stats.get("blkio_stats", {}).get(
                            "io_service_bytes_recursive"
                        )
                        or []
                    )
                    if item.get("op") == "Write"
                )
                return {
                    "scenario_id": scenario_id,
                    "component": component,
                    "container_name": details.get("Name", container.id).lstrip("/"),
                    "cpu_seconds": stats.get("cpu_stats", {}).get(
                        "cpu_usage", {}
                    ).get("total_usage", 0) / 1_000_000_000,
                    "memory_bytes": stats.get("memory_stats", {}).get("usage", 0),
                    "network_received_bytes": network_received,
                    "network_transmitted_bytes": network_transmitted,
                    "disk_written_bytes": disk_written,
                }
            except DockerError as error:
                if error.status != 404:
                    logger.warning("Container stats unavailable for %s: %s", container.id, error)
            except Exception:
                logger.exception("Failed to collect stats for container %s", container.id)
        snapshots = await asyncio.gather(*(collect(c) for c in containers))
        return [snapshot for snapshot in snapshots if snapshot is not None]

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
            # Repeated runs must not depend on registry availability, or change
            # a mutable image halfway through one experimental series.
            try:
                await self.client.images.inspect(tag)
                image = tag
            except DockerError as error:
                if error.status != 404:
                    raise
                image = await self.pull_image(tag)
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

    async def wait_for_container_file(self, container_id: str, path: str) -> None:
        container = await self.client.containers.get(container_id)
        while True:
            state = await self.get_container_state(container)
            if not state["running"]:
                raise RuntimeError(
                    f"Container {container_id} exited before writing {path} "
                    f"(exit code {state['exit_code']})"
                )
            try:
                archive = await container.get_archive(path)
            except DockerError as error:
                if error.status != 404:
                    raise
            else:
                archive.close()
                return
            await asyncio.sleep(0.5)

    async def write_container_file(self, container_id: str, path: str, contents: str) -> None:
        container = await self.client.containers.get(container_id)
        destination = pathlib.PurePosixPath(path)
        data = contents.encode()
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as archive:
            entry = tarfile.TarInfo(destination.name)
            entry.size = len(data)
            entry.mode = 0o644
            archive.addfile(entry, io.BytesIO(data))
        await container.put_archive(str(destination.parent), buffer.getvalue())

    async def signal_container(self, container_id: str, signal: str) -> None:
        container = await self.client.containers.get(container_id)
        await container.kill(signal=signal)

    async def rabbitmq_queue_pending(self, container_id: str, queue: str) -> int:
        container = await self.client.containers.get(container_id)
        command = await container.exec([
            "rabbitmqctl", "--quiet", "list_queues", "--formatter", "json",
            "name", "messages_ready", "messages_unacknowledged",
        ])
        chunks = []
        async with command.start() as stream:
            while (message := await stream.read_out()) is not None:
                if message.stream == 1:
                    chunks.append(message.data)
        if (await command.inspect())["ExitCode"] != 0:
            raise RuntimeError(f"Could not read queues from RabbitMQ container {container_id}")
        for row in json.loads(b"".join(chunks)):
            if row["name"] == queue:
                return int(row["messages_ready"]) + int(row["messages_unacknowledged"])
        raise RuntimeError(f"RabbitMQ queue {queue!r} is missing")

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
