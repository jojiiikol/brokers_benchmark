import asyncio
from dataclasses import replace
import io
import json
from pathlib import Path
import logging
import tempfile
import unittest

from src.docker_configurator import build_docker_payload
from src.components.producer.rabbit_mq.app import Producer
from src.components.common.load_profile import (
    LoadScheduler,
    LoadType as ComponentLoadType,
    publish_with_profile,
)
from src.components.common.producer_metrics import ProducerMetrics
from src.components.common.message_payload import (
    InvalidBenchmarkMessage,
    build_message,
    decode_message,
)
from src.components.consumer.rabbit_mq.app import LatencyMetrics
from src.components.consumer.nats.app import NatsConsumer
from src.components.consumer.rabbit_mq.app import Consumer as RabbitMQConsumer
from nats.js.api import AckPolicy
from src.scenario_service.dto.broker_config import (
    BrokerConfig,
    Brokers,
)
from src.scenario_service.creator.producer.producer_creator import ProducerCreator
from src.scenario_service.creator.consumer.consumer_creator import ConsumerCreator
from src.scenario_service.creator.brokers.rabbitmq_creator import RabbitMQCreator
from src.scenario_service.creator.brokers.kafka_creator import KafkaCreator
from src.scenario_service.dto.interaction_mode import InteractionMode
from src.scenario_service.creator.consumer.kafka_consumer_creator import KafkaConsumerCreator
from src.scenario_service.creator.brokers.nats_creator import NatsPullCreator, NatsPushCreator
from src.scenario_service.creator.consumer.nats_consumer_creator import (
    NatsPullConsumerCreator,
    NatsPushConsumerCreator,
)
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.producer_config import (
    GuaranteeType,
    LoadType,
    ProducerConfig,
)
from src.scenario_service.dto.scenario_config import ScenarioConfig
from src.scenario_service.service import ScenarioService, ScenarioStatus
from src.components.common.phase_control import MEASUREMENT_START, READY, WARMUP_DONE, WARMUP_START
from src.orchestrator import Orchestrator
from src.results.service import ScenarioResultService, _bounded_quantile


class FakeDockerService:
    def __init__(self):
        self.events = []

    async def start_container(self, container_id):
        self.events.append(("start", container_id))
        return True

    async def wait_for_container_file(self, container_id, path):
        self.events.append(("wait_for_file", container_id, path))

    async def write_container_file(self, container_id, path, contents):
        self.events.append(("release", container_id, path))

    async def signal_container(self, container_id, signal):
        self.events.append(("signal", container_id, signal))

    async def rabbitmq_queue_pending(self, container_id, queue):
        self.events.append(("queue_pending", container_id, queue))
        return 0

    async def clear_scenario_resources(self, scenario_id):
        self.events.append(("cleanup", scenario_id))


class FakeResultService:
    def __init__(self):
        self.saved = []
        self.final_scrapes = []

    async def wait_for_final_metrics(self, scenario_id, expected, timeout):
        self.final_scrapes.append((scenario_id, expected))
        return True

    async def save(self, scenario, final_status):
        self.saved.append((scenario.id, final_status))
        return Path(f"/tmp/{scenario.id}.json")


class RecordingScenarioObserver:
    def __init__(self):
        self.updates = []

    async def update(self, scenarios, active_task_count):
        self.updates.append((list(scenarios), active_task_count))


class FakeResultsCollector:
    async def collect(self, scenario_id):
        return {
            "source": "test",
            "messages": {"producer_accepted": 10, "consumer_received": 9},
            "latency_seconds": {"minimum": 0.001, "maximum": 0.005},
        }


def scenario_config(warmup_seconds=0, duration_seconds=0.001, drain_seconds=0):
    return ScenarioConfig(
        broker_config=BrokerConfig(Brokers.RABBITMQ),
        consumer_config=ConsumerConfig(1),
        producer_config=ProducerConfig(
            LoadType.CONSTANT,
            1,
            1,
            GuaranteeType.AT_MOST_ONCE,
            1,
        ),
        warmup_seconds=warmup_seconds,
        duration_seconds=duration_seconds,
        drain_seconds=drain_seconds,
    )


class ScenarioLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_scenario_warms_up_runs_and_cleans_up(self):
        docker = FakeDockerService()
        result_service = FakeResultService()
        service = ScenarioService(
            docker,
            result_service=result_service,
        )
        service.PHASE_RELEASE_LEAD_SECONDS = 0.05
        scenario = await service.create_scenario(scenario_config(drain_seconds=1))
        scenario.container_ids_by_role = {
            "broker": ["broker-1"],
            "consumer": ["consumer-1"],
            "producer": ["producer-1"],
        }

        await service.start_scenario(scenario)

        self.assertEqual(scenario.status, ScenarioStatus.FINISHED)
        self.assertIsNotNone(scenario.started_at)
        self.assertIsNotNone(scenario.warmup_finished_at)
        self.assertIsNotNone(scenario.measurement_started_at)
        self.assertTrue(scenario.drain_completed)
        self.assertTrue(scenario.final_metrics_collected)
        self.assertIsNotNone(scenario.drain_started_at)
        self.assertIsNotNone(scenario.finished_at)
        self.assertEqual(
            docker.events,
            [
                ("start", "broker-1"),
                ("start", "consumer-1"),
                ("start", "producer-1"),
                ("wait_for_file", "producer-1", READY),
                ("wait_for_file", "consumer-1", READY),
                ("release", "producer-1", WARMUP_START),
                ("wait_for_file", "producer-1", WARMUP_DONE),
                ("release", "producer-1", MEASUREMENT_START),
                ("wait_for_file", "producer-1", service.PRODUCER_COMPLETION_FILE),
                ("signal", "consumer-1", "SIGUSR2"),
                ("queue_pending", "broker-1", "scenario"),
                ("signal", "consumer-1", "SIGUSR1"),
                ("cleanup", scenario.id),
            ],
        )
        self.assertEqual(result_service.saved, [(scenario.id, "FINISHED")])
        self.assertEqual(result_service.final_scrapes, [(scenario.id, 2)])
        self.assertEqual(scenario.result_path, f"/tmp/{scenario.id}.json")

    async def test_starts_drain_only_after_all_producers_finish(self):
        class Docker(FakeDockerService):
            async def wait_for_container_file(self, container_id, path):
                if container_id == "slow-producer" and path == service.PRODUCER_COMPLETION_FILE:
                    waiting.set()
                    await finished.wait()

        waiting = asyncio.Event()
        finished = asyncio.Event()
        results = FakeResultService()
        service = ScenarioService(Docker(), result_service=results)
        service.PHASE_RELEASE_LEAD_SECONDS = 0.05
        scenario = await service.create_scenario(scenario_config())
        scenario.container_ids_by_role["broker"] = ["broker-1"]
        scenario.container_ids_by_role["producer"] = ["fast-producer", "slow-producer"]

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(service.start_scenario(scenario))
            try:
                await asyncio.wait_for(waiting.wait(), timeout=1)
                self.assertEqual(scenario.status, ScenarioStatus.RUNNING)
                self.assertIsNone(scenario.drain_started_at)
                self.assertEqual(results.saved, [])
            finally:
                finished.set()

        self.assertIsNotNone(scenario.drain_started_at)
        self.assertEqual(results.saved, [(scenario.id, "FINISHED")])

    async def test_drain_timeout_preserves_pending_count(self):
        class Docker(FakeDockerService):
            async def rabbitmq_queue_pending(self, container_id, queue):
                return 17

        service = ScenarioService(Docker(), result_service=FakeResultService())
        scenario = await service.create_scenario(scenario_config(drain_seconds=0.01))
        scenario.container_ids_by_role["broker"] = ["broker-1"]

        await service._drain_rabbitmq(scenario, ["slow-consumer"])

        self.assertFalse(scenario.drain_completed)
        self.assertEqual(scenario.drain_pending_messages, 17)
        self.assertEqual(service.docker_service.events[-1], ("signal", "slow-consumer", "SIGUSR1"))

    def test_rejects_invalid_stage_durations(self):
        with self.assertRaises(ValueError):
            scenario_config(warmup_seconds=-1)
        with self.assertRaises(ValueError):
            scenario_config(duration_seconds=0)
        with self.assertRaises(ValueError):
            scenario_config(drain_seconds=-1)


class OrchestratorShutdownTests(unittest.IsolatedAsyncioTestCase):
    def test_recognizes_when_every_submitted_scenario_is_terminal(self):
        orchestrator = Orchestrator()
        finished = type("ScenarioStub", (), {"status": ScenarioStatus.FINISHED})()
        failed = type("ScenarioStub", (), {"status": ScenarioStatus.ERROR})()
        orchestrator.scenario_storage = [finished, failed]

        self.assertTrue(orchestrator._all_scenarios_complete)

        orchestrator.scenario_tasks["still-running"] = object()
        self.assertFalse(orchestrator._all_scenarios_complete)

    async def test_shutdown_cancels_scenarios_and_cleans_dynamic_resources(self):
        class Docker:
            def __init__(self):
                self.cleaned = False

            async def clear_resources(self):
                self.cleaned = True

        async def wait_forever():
            await asyncio.Event().wait()

        orchestrator = Orchestrator()
        docker = Docker()
        orchestrator.docker_service = docker
        task = asyncio.create_task(wait_forever())
        orchestrator.scenario_tasks["scenario-1"] = task

        await orchestrator.shutdown()

        self.assertTrue(task.cancelled())
        self.assertTrue(docker.cleaned)
        self.assertEqual(orchestrator.scenario_tasks, {})

    async def test_scoped_shutdown_only_cleans_owned_scenarios(self):
        class Docker:
            def __init__(self):
                self.cleaned_scenarios = []

            async def clear_scenario_resources(self, scenario_id):
                self.cleaned_scenarios.append(scenario_id)

        orchestrator = Orchestrator()
        docker = Docker()
        orchestrator.docker_service = docker
        orchestrator.scenario_storage = [
            type("ScenarioStub", (), {"id": "owned-scenario"})(),
        ]

        await orchestrator.shutdown(clear_all_resources=False)

        self.assertEqual(docker.cleaned_scenarios, ["owned-scenario"])


class OrchestratorObserverTests(unittest.IsolatedAsyncioTestCase):
    async def test_notifies_an_optional_observer_after_scenario_creation(self):
        observer = RecordingScenarioObserver()
        orchestrator = Orchestrator(observer=observer)

        scenario_id = await orchestrator.create_scenario(scenario_config())

        self.assertEqual(len(observer.updates), 1)
        scenarios, active_task_count = observer.updates[0]
        self.assertEqual([scenario.id for scenario in scenarios], [scenario_id])
        self.assertEqual(active_task_count, 0)


class ScenarioResultServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_writes_configuration_and_collected_metrics_as_json(self):
        scenario = await ScenarioService(FakeDockerService()).create_scenario(
            scenario_config()
        )
        with tempfile.TemporaryDirectory() as directory:
            service = ScenarioResultService(
                directory=Path(directory), collector=FakeResultsCollector()
            )

            path = await service.save(scenario, final_status="FINISHED")

            artifact = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(artifact["scenario"]["id"], scenario.id)
        self.assertEqual(artifact["scenario"]["status"], "FINISHED")
        self.assertIn("drain_started_at", artifact["scenario"])
        self.assertEqual(
            artifact["scenario"]["configuration"]["broker_config"]["broker_type"],
            "rabbit_mq",
        )
        self.assertEqual(artifact["metrics"]["messages"]["producer_accepted"], 10)

    async def test_pub_sub_expected_deliveries_include_every_subscriber(self):
        class Collector:
            async def collect(self, scenario_id):
                return {"messages": {
                    "producer_accepted": 10, "consumer_received": 19, "consumer_duplicates": 1,
                }}

        config = replace(
            scenario_config(),
            interaction_mode=InteractionMode.PUB_SUB,
            consumer_config=ConsumerConfig(2),
        )
        scenario = await ScenarioService(FakeDockerService()).create_scenario(config)
        with tempfile.TemporaryDirectory() as directory:
            service = ScenarioResultService(directory=Path(directory), collector=Collector())
            path = await service.save(scenario, final_status="FINISHED")
            reliability = json.loads(path.read_text(encoding="utf-8"))["metrics"]["reliability"]
        self.assertEqual(reliability["expected_deliveries"], 20)
        self.assertEqual(reliability["lost_messages"], 2)

    def test_histogram_estimate_stays_within_sample_bounds(self):
        self.assertEqual(_bounded_quantile(0.49, 0.05, 0.18), 0.18)
        self.assertEqual(_bounded_quantile(0.10, 0.05, 0.18), 0.10)
        self.assertIsNone(_bounded_quantile(None, None, None))

    async def test_incomplete_drain_is_not_reported_as_loss(self):
        class Collector(FakeResultsCollector):
            async def collect(self, scenario_id):
                metrics = await super().collect(scenario_id)
                metrics["reliability"] = {"lost_messages": 1, "lost_share_percent": 10}
                return metrics

            async def received_at_drain(self, scenario_id):
                return 7

        scenario = await ScenarioService(FakeDockerService()).create_scenario(scenario_config())
        scenario.drain_completed = False
        scenario.drain_pending_messages = 1
        scenario.final_metrics_collected = True
        with tempfile.TemporaryDirectory() as directory:
            service = ScenarioResultService(directory=Path(directory), collector=Collector())
            path = await service.save(scenario, final_status="FINISHED")
            metrics = json.loads(path.read_text(encoding="utf-8"))["metrics"]
        self.assertIsNone(metrics["reliability"]["lost_messages"])
        self.assertEqual(metrics["drain"]["pending_at_last_check"], 1)
        self.assertEqual(metrics["drain"]["late_deliveries"], 2)


class ProducerPayloadTests(unittest.TestCase):
    def test_message_body_has_requested_size_in_both_phases(self):
        producer = Producer(tps=1, message_size=64)

        for phase in ("warmup", "measurement"):
            body = producer._create_payload(phase, sent_at=12345678901234567)
            self.assertEqual(len(body), 64 * 1024)
            message = decode_message(body)
            self.assertEqual(message.phase, phase)
            self.assertEqual(message.sent_at, 12345678901234567)

    def test_binary_header_preserves_metadata_and_exact_size(self):
        body = build_message(
            size_bytes=1024,
            phase="measurement",
            sent_at=123,
        )

        self.assertEqual(len(body), 1024)
        self.assertEqual(
            decode_message(body),
            type(decode_message(body))(
                phase="measurement",
                sent_at=123,
                message_id=0,
            ),
        )

    def test_rejects_non_benchmark_body(self):
        with self.assertRaises(InvalidBenchmarkMessage):
            decode_message(b"not a benchmark message")


class LoadProfileTests(unittest.TestCase):
    def test_linear_profile_reaches_both_requested_rates(self):
        scheduler = LoadScheduler(
            load_type=ComponentLoadType.LINEAR,
            measurement_duration_seconds=60,
            start_rps=100,
            end_rps=1000,
            step_duration_seconds=20,
        )

        self.assertEqual(scheduler.step_at(0).target_rps, 100)
        self.assertEqual(scheduler.step_at(20).target_rps, 550)
        self.assertEqual(scheduler.step_at(40).target_rps, 1000)

    def test_linear_profile_requires_equal_measurement_steps(self):
        with self.assertRaises(ValueError):
            LoadScheduler(
                load_type=ComponentLoadType.LINEAR,
                measurement_duration_seconds=61,
                start_rps=100,
                end_rps=1000,
                step_duration_seconds=20,
            )


class ConcurrentPublishTests(unittest.IsolatedAsyncioTestCase):
    async def test_max_throughput_reuses_workers_and_waits_for_publications(self):
        scheduler = LoadScheduler(ComponentLoadType.MAX_THROUGHPUT, 0.03)
        tasks = set()
        reports: list[int] = []
        publishes = 0

        async def publish(step):
            nonlocal publishes
            self.assertIsNone(step.target_rps)
            tasks.add(asyncio.current_task())
            await asyncio.sleep(0.001)
            publishes += 1

        await publish_with_profile(
            scheduler, 0.03, publish, lambda _step: None,
            max_in_flight=3, on_in_flight_changed=reports.append,
        )

        self.assertEqual(len(tasks), 3)
        self.assertGreater(publishes, 3)
        self.assertEqual(max(reports), 3)
        self.assertEqual(reports[-1], 0)

    async def test_reaps_completed_tasks_before_next_scheduled_publish(self):
        scheduler = LoadScheduler(
            load_type=ComponentLoadType.CONSTANT,
            measurement_duration_seconds=0.04,
            rps=100,
        )
        reported_in_flight: list[int] = []

        await publish_with_profile(
            scheduler=scheduler,
            duration_seconds=0.04,
            publish=lambda _step: asyncio.sleep(0),
            on_step=lambda _step: None,
            max_in_flight=1_000,
            on_in_flight_changed=reported_in_flight.append,
        )

        # Fast publications finish before the next 10 ms scheduled slot.
        # The window must therefore contain at most one task, regardless of
        # its configured capacity.
        self.assertEqual(max(reported_in_flight), 1)

    async def test_limits_concurrent_local_publish_operations(self):
        scheduler = LoadScheduler(
            load_type=ComponentLoadType.CONSTANT,
            measurement_duration_seconds=0.03,
            rps=10_000,
        )
        active = 0
        peak_active = 0
        published = 0

        async def publish(_step):
            nonlocal active, peak_active, published
            active += 1
            peak_active = max(peak_active, active)
            await asyncio.sleep(0.002)
            active -= 1
            published += 1

        await publish_with_profile(
            scheduler=scheduler,
            duration_seconds=0.03,
            publish=publish,
            on_step=lambda _step: None,
            max_in_flight=3,
        )

        self.assertEqual(peak_active, 3)
        self.assertGreater(published, 3)


class ProducerMetricsTests(unittest.IsolatedAsyncioTestCase):
    async def test_reports_only_local_publish_handoffs(self):
        stream = io.StringIO()
        test_logger = logging.Logger("producer-metrics-test")
        handler = logging.StreamHandler(stream)
        test_logger.addHandler(handler)
        metrics = ProducerMetrics(test_logger, report_interval_seconds=60)
        metrics.begin_phase("measurement")
        metrics.set_in_flight(1)
        await metrics.track(1024, lambda: asyncio.sleep(0))
        metrics.set_in_flight(0)
        metrics.log_final()

        output = stream.getvalue()
        self.assertIn("accepted=1", output)
        self.assertIn("errors=0", output)


class FakeProducerDockerService:
    def __init__(self):
        self.images_requested = 0
        self.container_configs = []

    async def get_image(self, **kwargs):
        self.images_requested += 1
        return kwargs["tag"]

    async def create_container(self, container_config, extra_labels):
        self.container_configs.append((container_config, extra_labels))
        return container_config.name


class ProducerScalingTests(unittest.IsolatedAsyncioTestCase):
    async def test_creates_one_independent_container_per_producer(self):
        docker = FakeProducerDockerService()
        config = scenario_config().producer_config
        config.count = 3
        config.rps = 1000
        creator = ProducerCreator(docker)

        container_ids = await creator.configurate(
            "scenario-1", config, Brokers.RABBITMQ, []
        )

        self.assertEqual(
            container_ids,
            ["producer-scenario-1-1", "producer-scenario-1-2", "producer-scenario-1-3"],
        )
        self.assertEqual(docker.images_requested, 1)
        for container_config, labels in docker.container_configs:
            self.assertEqual(
                container_config.labels,
                {"component": "producer", "prometheus.scrape": "true"},
            )
            self.assertEqual(labels, {"scenario_id": "scenario-1"})
            self.assertEqual(
                container_config.cmd[-6:],
                [
                    "--max-in-flight",
                    "100",
                    "--rps",
                    "1000",
                    "--payload_size",
                    "1",
                ],
            )
            guarantee_index = container_config.cmd.index("--guarantee-type")
            self.assertEqual(
                container_config.cmd[guarantee_index + 1],
                "at_most_once",
            )


class ProducerGuaranteeTests(unittest.TestCase):
    def test_allows_at_least_once(self):
        config = ProducerConfig(
            LoadType.CONSTANT,
            1,
            1,
            GuaranteeType.AT_LEAST_ONCE,
            1,
        )
        self.assertIs(config.guarantee_type, GuaranteeType.AT_LEAST_ONCE)

    def test_rejects_exactly_once(self):
        with self.assertRaisesRegex(ValueError, "exactly_once is not part"):
            ProducerConfig(
                LoadType.CONSTANT,
                1,
                1,
                GuaranteeType.EXACTLY_ONCE,
                1,
            )


class ConsumerScalingTests(unittest.IsolatedAsyncioTestCase):
    async def test_creates_consumers_with_unique_names(self):
        docker = FakeProducerDockerService()
        config = scenario_config().consumer_config
        config.count = 3
        creator = ConsumerCreator(docker)

        container_ids = await creator.configurate(
            "scenario-1", config, Brokers.RABBITMQ, []
        )

        self.assertEqual(
            container_ids,
            ["consumer-scenario-1-1", "consumer-scenario-1-2", "consumer-scenario-1-3"],
        )
        self.assertEqual(docker.images_requested, 1)

    async def test_applies_independent_cpu_and_memory_limits_to_each_container(self):
        docker = FakeProducerDockerService()
        config = scenario_config()
        producer = replace(config.producer_config, count=2, cpu_limit=1.5, memory_limit_mib=512)
        consumer = replace(config.consumer_config, count=2, cpu_limit=0.5, memory_limit_mib=256)

        await ProducerCreator(docker).configurate("scenario-1", producer, Brokers.RABBITMQ, [])
        await ConsumerCreator(docker).configurate("scenario-1", consumer, Brokers.RABBITMQ, [])

        limits = []
        for container, _ in docker.container_configs:
            payload, _, _ = build_docker_payload(container)
            host_config = payload["HostConfig"]
            limits.append((host_config["NanoCpus"], host_config["Memory"]))
        self.assertEqual(
            limits,
            [(1_500_000_000, 512 * 1024 ** 2)] * 2 + [(500_000_000, 256 * 1024 ** 2)] * 2,
        )


class RabbitMQInteractionModeTests(unittest.TestCase):
    def test_exposes_pub_sub_mode_to_clients(self):
        creator = RabbitMQCreator(FakeProducerDockerService())
        self.assertEqual(
            creator.interaction_mode_args(InteractionMode.PUB_SUB),
            ("--interaction-mode", "pub_sub"),
        )

    def test_uses_the_container_name_as_broker_hostname(self):
        creator = RabbitMQCreator(FakeProducerDockerService())
        self.assertEqual(
            creator.broker_hostname("scenario-1"),
            "rabbitmq-scenario-1",
        )

    def test_at_most_once_consumer_does_not_require_acknowledgements(self):
        consumer = RabbitMQConsumer(guarantee_type="at_most_once")

        self.assertFalse(consumer.uses_acknowledgements)


class KafkaCreatorTests(unittest.IsolatedAsyncioTestCase):
    async def test_creates_single_broker_with_sixteen_default_partitions(self):
        docker = FakeProducerDockerService()
        creator = KafkaCreator(docker)

        container_id = await creator.configurate(
            "scenario-1",
            BrokerConfig(Brokers.KAFKA),
        )

        self.assertEqual(container_id, "kafka-scenario-1")
        self.assertEqual(docker.images_requested, 1)
        container_config, labels = docker.container_configs[0]
        self.assertEqual(container_config.image, "apache/kafka:4.4.0")
        self.assertEqual(container_config.env["KAFKA_NUM_PARTITIONS"], "16")
        self.assertEqual(container_config.env["KAFKA_AUTO_CREATE_TOPICS_ENABLE"], "true")
        self.assertEqual(labels, {"scenario_id": "scenario-1"})
        self.assertEqual(creator.broker_hostname("scenario-1"), "kafka-scenario-1")

    async def test_configures_work_queue_partition_count(self):
        docker = FakeProducerDockerService()
        creator = KafkaCreator(docker)

        await creator.configurate(
            "scenario-1",
            BrokerConfig(Brokers.KAFKA),
            topic_partitions=4,
        )

        container_config, _ = docker.container_configs[0]
        self.assertEqual(container_config.env["KAFKA_NUM_PARTITIONS"], "4")

    async def test_rejects_more_than_sixteen_partitions(self):
        creator = KafkaCreator(FakeProducerDockerService())

        with self.assertRaisesRegex(ValueError, "between 1 and 16"):
            await creator.configurate(
                "scenario-1",
                BrokerConfig(Brokers.KAFKA),
                topic_partitions=17,
            )


class KafkaWorkQueueScenarioConfigTests(unittest.TestCase):
    def test_rejects_more_than_sixteen_kafka_work_queue_consumers(self):
        config = scenario_config()
        config.broker_config = BrokerConfig(Brokers.KAFKA)
        config.consumer_config.count = 17

        with self.assertRaisesRegex(ValueError, "at most 16 consumers"):
            ScenarioConfig(
                broker_config=config.broker_config,
                consumer_config=config.consumer_config,
                producer_config=config.producer_config,
                interaction_mode=InteractionMode.WORK_QUEUE,
                duration_seconds=1,
            )


class KafkaConsumerCreatorTests(unittest.IsolatedAsyncioTestCase):
    async def test_uses_one_group_for_work_queue_and_a_group_per_subscriber(self):
        docker = FakeProducerDockerService()
        config = ConsumerConfig(count=3)
        creator = KafkaConsumerCreator(docker)

        await creator.configurate(
            "scenario-1", config, Brokers.KAFKA, [], InteractionMode.WORK_QUEUE
        )
        work_queue_commands = [item[0].cmd for item in docker.container_configs]
        self.assertTrue(all(command[-2:] == ["--group-id", "scenario-workers"] for command in work_queue_commands))

        docker.container_configs.clear()
        await creator.configurate(
            "scenario-1", config, Brokers.KAFKA, [], InteractionMode.PUB_SUB
        )
        pub_sub_groups = [item[0].cmd[-1] for item in docker.container_configs]
        self.assertEqual(
            pub_sub_groups,
            ["scenario-subscriber-1", "scenario-subscriber-2", "scenario-subscriber-3"],
        )


class NatsCreatorTests(unittest.IsolatedAsyncioTestCase):
    async def test_creates_jetstream_broker_for_each_consumer_mode(self):
        for broker_type, creator_type, mode in (
            (Brokers.NATS_PULL, NatsPullCreator, "pull"),
            (Brokers.NATS_PUSH, NatsPushCreator, "push"),
        ):
            docker = FakeProducerDockerService()
            creator = creator_type(docker)
            container_id = await creator.configurate(
                "scenario-1", BrokerConfig(broker_type)
            )

            self.assertEqual(container_id, f"{broker_type.value}-scenario-1")
            container_config, _ = docker.container_configs[0]
            self.assertEqual(container_config.image, "nats:2.15.2")
            self.assertEqual(container_config.cmd, ["-js", "-m", "8222"])
            self.assertEqual(creator.consumer_mode_args(), ("--consumer-mode", mode))


class NatsConsumerCreatorTests(unittest.IsolatedAsyncioTestCase):
    async def test_creates_shared_workers_and_separate_pub_sub_consumers(self):
        config = ConsumerConfig(count=2)
        docker = FakeProducerDockerService()
        creator = NatsPullConsumerCreator(docker)

        await creator.configurate(
            "scenario-1", config, Brokers.NATS_PULL, [], InteractionMode.WORK_QUEUE
        )
        self.assertTrue(all(
            item[0].cmd[-4:] == ["--consumer-name", "scenario-workers", "--consumer-mode", "pull"]
            for item in docker.container_configs
        ))

        docker.container_configs.clear()
        push_creator = NatsPushConsumerCreator(docker)
        await push_creator.configurate(
            "scenario-1", config, Brokers.NATS_PUSH, [], InteractionMode.PUB_SUB
        )
        self.assertEqual(
            [item[0].cmd[-3] for item in docker.container_configs],
            ["scenario-subscriber-1", "scenario-subscriber-2"],
        )
        self.assertTrue(all(item[0].cmd[-1] == "push" for item in docker.container_configs))


class NatsConsumerModeTests(unittest.TestCase):
    def test_pull_uses_explicit_ack_and_push_uses_no_ack(self):
        pull_consumer = NatsConsumer(
            "nats", "4222", "scenario-workers", "pull", "work_queue",
            guarantee_type="at_most_once", metrics_window_size=5,
        )
        push_consumer = NatsConsumer(
            "nats", "4222", "scenario-workers", "push", "work_queue",
            guarantee_type="at_most_once", metrics_window_size=5,
        )

        self.assertEqual(pull_consumer._consumer_config().ack_policy, AckPolicy.EXPLICIT)
        self.assertEqual(push_consumer._consumer_config().ack_policy, AckPolicy.NONE)

    def test_push_at_least_once_uses_explicit_ack(self):
        consumer = NatsConsumer(
            "nats", "4222", "scenario-workers", "push", "work_queue",
            guarantee_type="at_least_once",
        )
        self.assertEqual(consumer._consumer_config().ack_policy, AckPolicy.EXPLICIT)



class LatencyMetricsTests(unittest.TestCase):
    def test_calculates_requested_percentiles_with_linear_interpolation(self):
        metrics = LatencyMetrics()
        for value in (4, 1, 3, 2):
            metrics.observe(value)

        summary = metrics.summary()

        self.assertEqual(summary["count"], 4)
        self.assertEqual(summary["p50_ms"], 2.5)
        self.assertEqual(summary["p75_ms"], 3.25)
        self.assertAlmostEqual(summary["p95_ms"], 3.85)
        self.assertAlmostEqual(summary["p99_ms"], 3.97)

    def test_returns_empty_percentiles_without_measurements(self):
        summary = LatencyMetrics().summary()
        self.assertEqual(summary["count"], 0)
        self.assertTrue(all(summary[f"p{p}_ms"] is None for p in (50, 75, 95, 99)))


class ConsumerMetricsReportingTests(unittest.TestCase):
    def test_rabbitmq_reports_and_resets_when_window_is_full(self):
        consumer = RabbitMQConsumer(
            metrics_window_size=3,
            metrics_log_every_windows=1,
        )
        for value in (1, 2, 3):
            consumer.metrics.observe(value)
        reports: list[bool] = []
        consumer._log_metrics = lambda *, final: reports.append(final)

        consumer._report_metrics_if_window_complete()

        self.assertEqual(reports, [False])
        self.assertEqual(consumer.metrics.count, 0)

    def test_rabbitmq_counts_valid_received_messages(self):
        consumer = RabbitMQConsumer(metrics_window_size=100)
        consumer._observe_message(
            build_message(size_bytes=32, phase="warmup", sent_at=1),
        )
        consumer._observe_message(
            build_message(size_bytes=32, phase="measurement", sent_at=2),
        )

        self.assertEqual(consumer.received_messages_total, 2)
        self.assertEqual(consumer.measurement_messages_total, 1)

