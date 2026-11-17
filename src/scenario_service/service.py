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
from src.scenario_service.creator.consumer.rabbitmq_consumer_creator import RabbitMQConsumerCreator
from src.scenario_service.creator.producer.rabbitmq_producer_creator import RabbitMQProducerCreator
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
from src.scenario_service.dto.interaction_mode import InteractionMode
from src.scenario_service.dto.producer_config import ProducerConfig, LoadType, GuaranteeType
from src.scenario_service.dto.scenario_config import ScenarioConfig
from src.results.service import ScenarioResultService

logger = logging.getLogger(__name__)


COMPONENTS_PATH = pathlib.Path(__file__).resolve().parent.parent / "components"

class ScenarioStatus(enum.Enum):
    CREATED = 'CREATED'
    INITIALIZING = 'INITIALIZING'
    PENDING = 'PENDING'
    STARTING = 'STARTING'
    WARMING_UP = 'WARMING_UP'
    RUNNING = 'RUNNING'
    DRAINING = 'DRAINING'
    STOPPING = 'STOPPING'
    CANCELLED = 'CANCELLED'
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
    drain_started_at: datetime.datetime | None = None
    result_path: str | None = None
    error: str | None = None
    container_ids_by_role: dict[str, list[str]] = dataclasses.field(
        default_factory=lambda: {"broker": [], "consumer": [], "producer": []}
    )

class ScenarioService:
    MAX_RUNNING_SCENARIO_COUNT: int = 5
    TIMEOUT_SCENARIO_SECONDS: int = 45
    PRODUCER_COMPLETION_TIMEOUT_SECONDS: int = 60
    PRODUCER_COMPLETION_FILE = "/tmp/benchmark-publishing-finished"

    def __init__(
            self,
            # configurator: ComponentConfigurator,
            docker_service: SdkDockerService | None = None,
            result_service: ScenarioResultService | None = None,
    ):
        self.docker_service = docker_service or SdkDockerService()
        self.result_service = result_service or ScenarioResultService()
        # self.configurator = ComponentConfigurator(docker_service)
        self.producer_creators: dict[Brokers, BaseProducerCreator] = {
            Brokers.RABBITMQ: RabbitMQProducerCreator(self.docker_service),
            Brokers.KAFKA: KafkaProducerCreator(self.docker_service),
            Brokers.NATS_PULL: NatsPullProducerCreator(self.docker_service),
            Brokers.NATS_PUSH: NatsPushProducerCreator(self.docker_service),
        }
        self.consumer_creators: dict[Brokers, BaseConsumerCreator] = {
            Brokers.RABBITMQ: RabbitMQConsumerCreator(self.docker_service),
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
            broker_kwargs = {}
            if (
                scenario.config.broker_config.broker_type is Brokers.KAFKA
                and scenario.config.interaction_mode is InteractionMode.WORK_QUEUE
            ):
                broker_kwargs["topic_partitions"] = scenario.config.consumer_config.count
            broker_container = await broker_creator.configurate(
                scenario_id=scenario_id,
                config=scenario.config.broker_config,
                **broker_kwargs,
            )
            scenario.containers.append(broker_container)
            scenario.container_ids_by_role["broker"].append(broker_container)
            broker_hostname = broker_creator.broker_hostname(scenario_id)

            producer_args = [
                "--broker-host", broker_hostname,
                "--broker-port", str(self._broker_port(scenario.config.broker_config.broker_type)),
                "--warmup-seconds", str(scenario.config.warmup_seconds),
                "--duration-seconds", str(scenario.config.duration_seconds),
                "--completion-file", self.PRODUCER_COMPLETION_FILE,
                *broker_creator.interaction_mode_args(scenario.config.interaction_mode),
            ]

            producer_containers = await producer_creator.configurate(
                scenario_id=scenario_id,
                config=scenario.config.producer_config,
                broker_type=scenario.config.broker_config.broker_type,
                docker_args=producer_args,
            )
            scenario.containers.extend(producer_containers)
            scenario.container_ids_by_role["producer"].extend(producer_containers)

            consumer_args = [
                "--broker-host", broker_hostname,
                "--broker-port", str(self._broker_port(scenario.config.broker_config.broker_type)),
                *broker_creator.interaction_mode_args(scenario.config.interaction_mode),
                "--guarantee-type", scenario.config.producer_config.guarantee_type.value,
            ]

            consumer_containers = await consumer_creator.configurate(
                scenario_id=scenario_id,
                config=scenario.config.consumer_config,
                broker_type=scenario.config.broker_config.broker_type,
                docker_args=consumer_args,
                interaction_mode=scenario.config.interaction_mode,
            )
            scenario.containers.extend(consumer_containers)
            scenario.container_ids_by_role["consumer"].extend(consumer_containers)

            await network.connect({"Container": broker_container})
            for producer_container in producer_containers:
                await network.connect({"Container": producer_container})
            for consumer_container in consumer_containers:
                await network.connect({"Container": consumer_container})

            # Prometheus and Grafana are permanent Compose services. Only
            # workload components join their shared network; brokers stay
            # isolated in a scenario network.
            for component_container in (*producer_containers, *consumer_containers):
                await self.docker_service.connect_to_observability_network(
                    component_container
                )

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
        """Run one scenario through warm-up, measurement, draining and shutdown."""
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

            async with asyncio.timeout(self.PRODUCER_COMPLETION_TIMEOUT_SECONDS):
                async with asyncio.TaskGroup() as producers:
                    for container_id in scenario.container_ids_by_role["producer"]:
                        producers.create_task(
                            self.docker_service.wait_for_container_file(
                                container_id, self.PRODUCER_COMPLETION_FILE
                            )
                        )

            scenario.status = ScenarioStatus.DRAINING
            scenario.drain_started_at = datetime.datetime.now(datetime.UTC)
            logger.info(
                "Scenario %s is draining messages for %s seconds",
                scenario.id,
                scenario.config.drain_seconds,
            )
            await asyncio.sleep(scenario.config.drain_seconds)
            scenario.status = ScenarioStatus.STOPPING
            scenario.finished_at = datetime.datetime.now(datetime.UTC)
            await self._save_result(scenario, final_status=ScenarioStatus.FINISHED)
            await self.docker_service.clear_scenario_resources(scenario.id)
            scenario.status = ScenarioStatus.FINISHED
            return scenario.id
        except asyncio.CancelledError:
            scenario.status = ScenarioStatus.STOPPING
            scenario.finished_at = datetime.datetime.now(datetime.UTC)
            await self._save_result(scenario, final_status=ScenarioStatus.CANCELLED)
            await self.docker_service.clear_scenario_resources(scenario.id)
            scenario.status = ScenarioStatus.CANCELLED
            raise
        except Exception as e:
            logger.exception("Scenario %s failed", scenario.id)
            scenario.status = ScenarioStatus.ERROR
            scenario.error = str(e)
            scenario.finished_at = datetime.datetime.now(datetime.UTC)
            await self._save_result(scenario, final_status=ScenarioStatus.ERROR)
            await self.docker_service.clear_scenario_resources(scenario.id)

    async def cancel_scenario(self, scenario: Scenario) -> None:
        """Cancel a scenario that has not been assigned a running task yet."""
        if scenario.status in {
            ScenarioStatus.CANCELLED,
            ScenarioStatus.ERROR,
            ScenarioStatus.FINISHED,
        }:
            return

        scenario.status = ScenarioStatus.STOPPING
        scenario.finished_at = datetime.datetime.now(datetime.UTC)
        await self._save_result(scenario, final_status=ScenarioStatus.CANCELLED)
        await self.docker_service.clear_scenario_resources(scenario.id)
        scenario.status = ScenarioStatus.CANCELLED

    async def _start_role(self, scenario: Scenario, role: str) -> None:
        for container_id in scenario.container_ids_by_role[role]:
            if not await self.docker_service.start_container(container_id):
                raise RuntimeError(f"Failed to start {role} container {container_id}")

    async def _save_result(
            self,
            scenario: Scenario,
            final_status: ScenarioStatus,
    ) -> None:
        try:
            path = await self.result_service.save(scenario, final_status.value)
            scenario.result_path = str(path)
        except Exception:
            # A failed report must not prevent resource cleanup or alter the
            # experiment status.
            logger.exception("Failed to save result for scenario %s", scenario.id)



