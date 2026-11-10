from src.docker_configurator import ContainerConfig
from src.docker_configurator.schema import ResourceLimits
from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.creator.base.abstract_broker import BaseBrokerCreator
from src.scenario_service.dto.broker_config import BrokerConfig
from src.scenario_service.dto.interaction_mode import InteractionMode


class NatsJetStreamCreator(BaseBrokerCreator):
    """Create one NATS server with JetStream enabled for a scenario."""

    consumer_mode: str

    async def configurate(
            self,
            scenario_id: str,
            config: BrokerConfig,
            *args,
            **kwargs,
    ) -> str | None:
        broker_name = f"{config.broker_type.value}-{scenario_id}"
        image = await self.docker_service.get_image(tag="nats:2.15.2")
        return await self.docker_service.create_container(
            container_config=ContainerConfig(
                image=image,
                name=broker_name,
                network_name=f"network-{scenario_id}",
                cmd=["-js", "-m", "8222"],
                labels={"component": "broker"},
                resources=ResourceLimits(
                    memory=1 * 1024 * 1024 * 1024,
                    nano_cpus=1 * 1_000_000_000,
                ),
            ),
            extra_labels={"scenario_id": scenario_id},
        )

    def interaction_mode_args(self, mode: InteractionMode) -> tuple[str, ...]:
        if mode not in (InteractionMode.WORK_QUEUE, InteractionMode.PUB_SUB):
            raise ValueError(f"NATS does not support interaction mode {mode.value}")
        return ("--interaction-mode", mode.value)

    def consumer_mode_args(self) -> tuple[str, ...]:
        return ("--consumer-mode", self.consumer_mode)


class NatsPullCreator(NatsJetStreamCreator):
    consumer_mode = "pull"


class NatsPushCreator(NatsJetStreamCreator):
    consumer_mode = "push"
