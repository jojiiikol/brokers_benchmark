import asyncio
import dataclasses
import datetime
import enum
import logging
import os
import pathlib
import uuid

from src.config import CONTAINERS_NAMESPACE
from src.docker_configurator import ContainerConfig
from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.creator.base.abstract_broker import BaseBrokerCreator
from src.scenario_service.creator.base.abstract_consumer import BaseConsumerCreator
from src.scenario_service.creator.base.abstract_producer import BaseProducerCreator
# from src.scenario_service.configurator import ComponentConfigurator
from src.scenario_service.creator.base.abstract_service import BaseServiceCreator
from src.scenario_service.creator.brokers.rabbitmq_creator import RabbitMQCreator
from src.scenario_service.creator.consumer.consumer_creator import ConsumerCreator
from src.scenario_service.creator.producer.producer_creator import ProducerCreator
from src.scenario_service.creator.brokers.kafka_creator import KafkaCreator
from src.scenario_service.creator.consumer.kafka_consumer_creator import KafkaConsumerCreator
from src.scenario_service.creator.producer.kafka_producer_creator import KafkaProducerCreator
from src.scenario_service.creator.brokers.nats_creator import NatsPullCreator, NatsPushCreator
from src.scenario_service.creator.consumer.nats_consumer_creator import (
    NatsPullConsumerCreator,
    NatsPushConsumerCreator,
)
from src.scenario_service.creator.producer.nats_producer_creator import (
    NatsPullProducerCreator,
    NatsPushProducerCreator,
)
from src.scenario_service.dto.broker_config import BrokerConfig, Brokers
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.producer_config import ProducerConfig, LoadType, GuaranteeType
from src.scenario_service.dto.scenario_config import ScenarioConfig

logger = logging.getLogger(__name__)


COMPONENTS_PATH = pathlib.Path(__file__).resolve().parent.parent / "components"

class ScenarioStatus(enum.Enum):
    CREATED = 'CREATED'
    INITIALIZING = 'INITIALIZING'
    PENDING = 'PENDING'
    STARTING = 'STARTING'
    WARMING_UP = 'WARMING_UP'
    RUNNING = 'RUNNING'
    STOPPING = 'STOPPING'
    ERROR = 'ERROR'
    FINISHED = 'FINISHED'

@dataclasses.dataclass
class Scenario:
    id: str
    name: str
    containers: list
    config: ScenarioConfig
    finished_at: datetime.datetime | None

    status: ScenarioStatus = ScenarioStatus.CREATED
    created_at: datetime.datetime = dataclasses.field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC)
    )
    started_at: datetime.datetime | None = None
    warmup_finished_at: datetime.datetime | None = None
    error: str | None = None
    container_ids_by_role: dict[str, list[str]] = dataclasses.field(
        default_factory=lambda: {"broker": [], "consumer": [], "producer": []}
    )

class ScenarioService:
    MAX_RUNNING_SCENARIO_COUNT: int = 5
    TIMEOUT_SCENARIO_SECONDS: int = 45

    def __init__(
            self,
            # configurator: ComponentConfigurator,
            docker_service: SdkDockerService | None = None,

    ):
        self.docker_service = docker_service or SdkDockerService()
        # self.configurator = ComponentConfigurator(docker_service)
        self.producer_creators: dict[Brokers, BaseProducerCreator] = {
            Brokers.RABBITMQ: ProducerCreator(self.docker_service),
            Brokers.KAFKA: KafkaProducerCreator(self.docker_service),
            Brokers.NATS_PULL: NatsPullProducerCreator(self.docker_service),
            Brokers.NATS_PUSH: NatsPushProducerCreator(self.docker_service),
        }
        self.consumer_creators: dict[Brokers, BaseConsumerCreator] = {
            Brokers.RABBITMQ: ConsumerCreator(self.docker_service),
            Brokers.KAFKA: KafkaConsumerCreator(self.docker_service),
            Brokers.NATS_PULL: NatsPullConsumerCreator(self.docker_service),
            Brokers.NATS_PUSH: NatsPushConsumerCreator(self.docker_service),
        }
        self.broker_creators: dict[Brokers, BaseBrokerCreator] = {
            Brokers.RABBITMQ: RabbitMQCreator(self.docker_service),
            Brokers.KAFKA: KafkaCreator(self.docker_service),
            Brokers.NATS_PULL: NatsPullCreator(self.docker_service),
            Brokers.NATS_PUSH: NatsPushCreator(self.docker_service),
        }

    def _get_creators(
            self,
            config: ScenarioConfig,
    ) -> tuple[BaseBrokerCreator, BaseProducerCreator, BaseConsumerCreator]:
        broker_type = config.broker_config.broker_type
        component_brokers = {
            config.producer_config.broker_connection.broker_type,
            config.consumer_config.broker_connection.broker_type,
        }
        if component_brokers != {broker_type}:
            raise ValueError(
                "The broker type in producer and consumer connections must "
                "match the scenario broker type"
            )
        try:
            return (
                self.broker_creators[broker_type],
                self.producer_creators[broker_type],
                self.consumer_creators[broker_type],
            )
        except KeyError as error:
            raise ValueError(f"Broker {broker_type.value} is not implemented") from error

    def _component_count(
            self,
            component_name: str,
            labels: list[str],
    ) -> int:
        return len(list(filter(lambda item: item.get("component") == component_name, labels)))


    async def initialize_scenario(
            self,
            scenario: Scenario,
    ) -> Scenario:
        scenario_id = scenario.id
        scenario.status = ScenarioStatus.INITIALIZING
        try:
            broker_creator, producer_creator, consumer_creator = self._get_creators(
                scenario.config
            )
            network = await self.docker_service.create_network(
                name=f"network-{scenario_id}",
                labels={
                    "namespace": CONTAINERS_NAMESPACE,
                    "scenario_id": scenario_id,
                },
            )

            logger.info(
                "Creating broker: type=%s scenario_id=%s",
                scenario.config.broker_config.broker_type.value,
                scenario_id,
            )
            broker_container = await broker_creator.configurate(
                scenario_id=scenario_id,
                config=scenario.config.broker_config,
            )
            scenario.containers.append(broker_container)
            scenario.container_ids_by_role["broker"].append(broker_container)

            producer_config = dataclasses.replace(
                scenario.config.producer_config,
                docker_args=[*scenario.config.producer_config.docker_args,
                "--broker-host", f"{scenario.config.broker_config.broker_type.value}-{scenario_id}",
                "--broker-port", str(self._broker_port(scenario.config.broker_config.broker_type)),
                "--warmup-seconds", str(scenario.config.warmup_seconds),
                "--duration-seconds", str(scenario.config.duration_seconds),
                *broker_creator.interaction_mode_args(scenario.config.interaction_mode),
                ],
            )

            producer_containers = await producer_creator.configurate(
                scenario_id=scenario_id,
                config=producer_config,
            )
            scenario.containers.extend(producer_containers)
            scenario.container_ids_by_role["producer"].extend(producer_containers)

            consumer_config = dataclasses.replace(
                scenario.config.consumer_config,
                docker_args=[*scenario.config.consumer_config.docker_args,
                "--broker-host", f"{scenario.config.broker_config.broker_type.value}-{scenario_id}",
                "--broker-port", str(self._broker_port(scenario.config.broker_config.broker_type)),
                *broker_creator.interaction_mode_args(scenario.config.interaction_mode),
                ],
            )

            consumer_containers = await consumer_creator.configurate(
                scenario_id=scenario_id,
                config=consumer_config,
                interaction_mode=scenario.config.interaction_mode,
            )
            scenario.containers.extend(consumer_containers)
            scenario.container_ids_by_role["consumer"].extend(consumer_containers)

            await network.connect({"Container": broker_container})
            for producer_container in producer_containers:
                await network.connect({"Container": producer_container})
            for consumer_container in consumer_containers:
                await network.connect({"Container": consumer_container})

            if not await self._validate_scenario(scenario):
                raise ValueError("Component in scenario not built")

            scenario.status = ScenarioStatus.PENDING
        except Exception as e:
            logger.exception("Failed to initialize scenario %s", scenario.id)
            scenario.error = str(e)
            scenario.status = ScenarioStatus.ERROR
            await self.docker_service.clear_scenario_resources(scenario.id)

        return scenario

    @staticmethod
    def _broker_port(broker_type: Brokers) -> int:
        if broker_type is Brokers.RABBITMQ:
            return 5672
        if broker_type is Brokers.KAFKA:
            return 9092
        if broker_type in (Brokers.NATS_PULL, Brokers.NATS_PUSH):
            return 4222
        raise ValueError(f"Broker {broker_type.value} is not implemented")

    async def _validate_scenario(self, scenario: Scenario) -> bool:
        expected_counts = {
            "producer": scenario.config.producer_config.count,
            "consumer": scenario.config.consumer_config.count,
            "broker": 1,
        }

        for component, expected_count in expected_counts.items():
            resources = await self.docker_service._get_resources_by_labels(
                resource_collection=self.docker_service.client.containers,
                labels={
                    "namespace": CONTAINERS_NAMESPACE,
                    "component": component,
                    "scenario_id": scenario.id,
                },
                all=True,
            )

            actual_count = len(resources)

            if actual_count != expected_count:
                logger.error(
                    "Invalid component count for %s: expected=%s, actual=%s",
                    component,
                    expected_count,
                    actual_count,
                )
                return False

        return True

    async def create_scenario(
            self,
            scenario_config: ScenarioConfig,
    ) -> Scenario:

        scenario_id = str(uuid.uuid4())
        scenario = Scenario(
            id=scenario_id,
            name="scenario - {}".format(scenario_id),
            containers=[],
            config=scenario_config,
            finished_at=None,
            status=ScenarioStatus.CREATED,
            created_at=datetime.datetime.now(datetime.UTC),
        )

        logger.info(f"Created scenario {scenario.id}")
        return scenario


    async def start_scenario(self, scenario: Scenario) -> str | None:
        """Run one scenario through warm-up, measurement and shutdown."""
        scenario.status = ScenarioStatus.STARTING
        scenario.started_at = datetime.datetime.now(datetime.UTC)
        try:
            await self._start_role(scenario, "broker")
            # The consumer must be connected before the producer sends either
            # warm-up or measured messages.
            await self._start_role(scenario, "consumer")
            await self._start_role(scenario, "producer")

            scenario.status = ScenarioStatus.WARMING_UP
            logger.info(
                "Scenario %s is warming up with message traffic for %s seconds",
                scenario.id,
                scenario.config.warmup_seconds,
            )
            await asyncio.sleep(scenario.config.warmup_seconds)
            scenario.warmup_finished_at = datetime.datetime.now(datetime.UTC)

            scenario.status = ScenarioStatus.RUNNING
            logger.info("Scenario %s is running for %s seconds", scenario.id,
                        scenario.config.duration_seconds)
            await asyncio.sleep(scenario.config.duration_seconds)

            scenario.status = ScenarioStatus.STOPPING
            await self.docker_service.clear_scenario_resources(scenario.id)
            scenario.status = ScenarioStatus.FINISHED
            scenario.finished_at = datetime.datetime.now(datetime.UTC)
            return scenario.id
        except asyncio.CancelledError:
            scenario.status = ScenarioStatus.STOPPING
            await self.docker_service.clear_scenario_resources(scenario.id)
            scenario.status = ScenarioStatus.ERROR
            scenario.error = "Scenario task was cancelled"
            scenario.finished_at = datetime.datetime.now(datetime.UTC)
            raise
        except Exception as e:
            logger.exception("Scenario %s failed", scenario.id)
            scenario.status = ScenarioStatus.ERROR
            scenario.error = str(e)
            await self.docker_service.clear_scenario_resources(scenario.id)
            scenario.finished_at = datetime.datetime.now(datetime.UTC)

    async def _start_role(self, scenario: Scenario, role: str) -> None:
        for container_id in scenario.container_ids_by_role[role]:
            if not await self.docker_service.start_container(container_id):
                raise RuntimeError(f"Failed to start {role} container {container_id}")




async def main():
    service = ScenarioService()
    scenario = await service.create_scenario(
        scenario_config=ScenarioConfig(
            broker_config=BrokerConfig(
                broker_type=Brokers.RABBITMQ
            ),
            consumer_config=ConsumerConfig(
                count=2,
                docker_args=[]
            ),
            producer_config=ProducerConfig(
                load_type=LoadType.linear,
                payload_size=1024,
                rps=2,
                guarantee_type=GuaranteeType.EXACTLY_ONCE,
                count=1,
                docker_args=["--rps", "5"]
            )
        )
    )
    print(scenario)



if __name__ == "__main__":
    asyncio.run(main())



