from src.docker_configurator import ContainerConfig
from src.docker_configurator.schema import ResourceLimits
from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.creator.base.abstract_broker import BaseBrokerCreator
from src.scenario_service.dto.broker_config import BrokerConfig
from src.scenario_service.dto.interaction_mode import InteractionMode

class KafkaCreator(BaseBrokerCreator):
    """Create one isolated, single-node Kafka broker for a scenario."""

    TOPIC_PARTITIONS = 16

    async def configurate(
            self,
            scenario_id: str,
            config: BrokerConfig,
            *args,
            **kwargs,
    ) -> str | None:
        broker_name = f"kafka-{scenario_id}"
        image = await self.docker_service.get_image(tag="apache/kafka:4.4.0")
        return await self.docker_service.create_container(
            container_config=ContainerConfig(
                image=image,
                name=broker_name,
                network_name=f"network-{scenario_id}",
                labels={"component": "broker"},
                env={
                    "KAFKA_NODE_ID": "1",
                    "KAFKA_PROCESS_ROLES": "broker,controller",
                    "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
                    "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{broker_name}:9092",
                    "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
                    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": (
                        "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
                    ),
                    "KAFKA_CONTROLLER_QUORUM_VOTERS": f"1@{broker_name}:9093",
                    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
                    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
                    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
                    "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
                    "KAFKA_NUM_PARTITIONS": str(self.TOPIC_PARTITIONS),
                    "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
                },
                resources=ResourceLimits(
                    memory=1 * 1024 * 1024 * 1024,
                    nano_cpus=1 * 1_000_000_000,
                ),
            ),
            extra_labels={"scenario_id": scenario_id},
        )

    def interaction_mode_args(self, mode: InteractionMode) -> tuple[str, ...]:
        if mode not in (InteractionMode.WORK_QUEUE, InteractionMode.PUB_SUB):
            raise ValueError(f"Kafka does not support interaction mode {mode.value}")
        return ("--interaction-mode", mode.value)
