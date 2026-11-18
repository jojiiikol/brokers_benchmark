import unittest

from src.cli import DEFAULT_CONSTANT_RPS, parse_scenario_config
from src.scenario_service.dto.broker_config import Brokers
from src.scenario_service.dto.interaction_mode import InteractionMode
from src.scenario_service.dto.producer_config import GuaranteeType, LoadType


class CliTests(unittest.TestCase):
    def test_defaults_build_runnable_rabbitmq_scenario(self):
        config = parse_scenario_config([])

        self.assertIs(config.broker_config.broker_type, Brokers.RABBITMQ)
        self.assertIs(config.interaction_mode, InteractionMode.WORK_QUEUE)
        self.assertIs(config.producer_config.load_type, LoadType.CONSTANT)
        self.assertEqual(config.producer_config.rps, DEFAULT_CONSTANT_RPS)
        self.assertEqual(config.producer_config.payload_size, 1)

    def test_parses_complete_linear_kafka_scenario(self):
        config = parse_scenario_config([
            "--broker", "kafka",
            "--interaction-mode", "pub_sub",
            "--load-type", "linear",
            "--rps", "100",
            "--end-rps", "1000",
            "--step-duration-seconds", "10",
            "--payload-size-kib", "32",
            "--max-in-flight", "500",
            "--producers", "2",
            "--consumers", "3",
            "--guarantee", "at_least_once",
            "--warmup-seconds", "5",
            "--duration-seconds", "60",
            "--drain-seconds", "7",
            "--producer-cpu", "1.5",
            "--producer-memory-mib", "512",
            "--consumer-cpu", "0.5",
            "--consumer-memory-mib", "256",
        ])

        self.assertIs(config.broker_config.broker_type, Brokers.KAFKA)
        self.assertIs(config.interaction_mode, InteractionMode.PUB_SUB)
        self.assertIs(config.producer_config.load_type, LoadType.LINEAR)
        self.assertIs(
            config.producer_config.guarantee_type,
            GuaranteeType.AT_LEAST_ONCE,
        )
        self.assertEqual(config.producer_config.count, 2)
        self.assertEqual(config.consumer_config.count, 3)
        self.assertEqual(config.producer_config.memory_limit_mib, 512)
        self.assertEqual(config.consumer_config.cpu_limit, 0.5)
        self.assertEqual(config.drain_seconds, 7)

    def test_parses_max_throughput_without_rps(self):
        config = parse_scenario_config([
            "--broker", "nats_push",
            "--load-type", "max_throughput",
        ])

        self.assertIs(config.producer_config.load_type, LoadType.MAX_THROUGHPUT)
        self.assertIsNone(config.producer_config.rps)

    def test_rejects_linear_load_without_starting_rps(self):
        with self.assertRaises(SystemExit):
            parse_scenario_config([
                "--load-type", "linear",
                "--end-rps", "1000",
                "--step-duration-seconds", "10",
            ])

    def test_rejects_rps_for_max_throughput(self):
        with self.assertRaises(SystemExit):
            parse_scenario_config([
                "--load-type", "max_throughput",
                "--rps", "1000",
            ])

    def test_rejects_zero_consumers_before_starting_docker(self):
        with self.assertRaises(SystemExit):
            parse_scenario_config(["--consumers", "0"])
