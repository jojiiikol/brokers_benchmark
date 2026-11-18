"""Standalone CLI entrypoint for running one benchmark scenario."""

import argparse
import asyncio
from collections.abc import Sequence

from src.benchmark_runtime import run_scenario
from src.scenario_service.dto.broker_config import BrokerConfig, Brokers
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.interaction_mode import InteractionMode
from src.scenario_service.dto.producer_config import (
    GuaranteeType,
    LoadType,
    ProducerConfig,
)
from src.scenario_service.dto.scenario_config import ScenarioConfig


DEFAULT_CONSTANT_RPS = 1_000


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a message-broker benchmark scenario.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--broker",
        choices=_values(Brokers),
        default=Brokers.RABBITMQ.value,
        help="Broker implementation to benchmark.",
    )
    parser.add_argument(
        "--interaction-mode",
        choices=_values(InteractionMode),
        default=InteractionMode.WORK_QUEUE.value,
        help="Message delivery topology.",
    )

    load = parser.add_argument_group("load profile")
    load.add_argument(
        "--load-type",
        choices=(
            LoadType.CONSTANT.value,
            LoadType.LINEAR.value,
            LoadType.MAX_THROUGHPUT.value,
        ),
        default=LoadType.CONSTANT.value,
    )
    load.add_argument(
        "--rps",
        type=int,
        help=(
            "Target RPS for constant load (defaults to "
            f"{DEFAULT_CONSTANT_RPS}) or starting RPS for linear load. "
            "RPS is applied to each producer."
        ),
    )
    load.add_argument("--end-rps", type=int, help="Final RPS for linear load.")
    load.add_argument(
        "--step-duration-seconds",
        type=float,
        help="Duration of each linear-load step.",
    )
    load.add_argument(
        "--payload-size-kib",
        type=int,
        default=1,
        help="Message payload size in KiB.",
    )
    load.add_argument(
        "--max-in-flight",
        type=int,
        default=100,
        help="Maximum concurrent publish operations per producer.",
    )

    clients = parser.add_argument_group("clients and delivery")
    clients.add_argument("--producers", type=int, default=1)
    clients.add_argument("--consumers", type=int, default=1)
    clients.add_argument(
        "--guarantee",
        choices=(
            GuaranteeType.AT_MOST_ONCE.value,
            GuaranteeType.AT_LEAST_ONCE.value,
        ),
        default=GuaranteeType.AT_MOST_ONCE.value,
    )

    phases = parser.add_argument_group("scenario phases")
    phases.add_argument("--warmup-seconds", type=float, default=5)
    phases.add_argument("--duration-seconds", type=float, default=60)
    phases.add_argument("--drain-seconds", type=float, default=10)

    resources = parser.add_argument_group("per-container resource limits")
    resources.add_argument("--producer-cpu", type=float, default=2.0)
    resources.add_argument("--producer-memory-mib", type=int)
    resources.add_argument("--consumer-cpu", type=float, default=2.0)
    resources.add_argument("--consumer-memory-mib", type=int)
    return parser


def parse_scenario_config(argv: Sequence[str] | None = None) -> ScenarioConfig:
    """Parse CLI arguments and return a fully validated scenario config."""
    parser = create_parser()
    args = parser.parse_args(argv)
    try:
        return scenario_config_from_args(args)
    except ValueError as error:
        parser.error(str(error))


def scenario_config_from_args(args: argparse.Namespace) -> ScenarioConfig:
    if args.consumers <= 0:
        raise ValueError("consumers must be greater than zero")

    load_type = LoadType(args.load_type)
    rps = args.rps
    if load_type is LoadType.CONSTANT and rps is None:
        rps = DEFAULT_CONSTANT_RPS
    if load_type is LoadType.LINEAR and rps is None:
        raise ValueError("linear load requires --rps as its starting rate")

    return ScenarioConfig(
        broker_config=BrokerConfig(broker_type=Brokers(args.broker)),
        consumer_config=ConsumerConfig(
            count=args.consumers,
            cpu_limit=args.consumer_cpu,
            memory_limit_mib=args.consumer_memory_mib,
        ),
        producer_config=ProducerConfig(
            load_type=load_type,
            payload_size=args.payload_size_kib,
            rps=rps,
            end_rps=args.end_rps,
            step_duration_seconds=args.step_duration_seconds,
            guarantee_type=GuaranteeType(args.guarantee),
            count=args.producers,
            max_in_flight=args.max_in_flight,
            cpu_limit=args.producer_cpu,
            memory_limit_mib=args.producer_memory_mib,
        ),
        warmup_seconds=args.warmup_seconds,
        duration_seconds=args.duration_seconds,
        drain_seconds=args.drain_seconds,
        interaction_mode=InteractionMode(args.interaction_mode),
    )


def _values(enum_type: type) -> tuple[str, ...]:
    return tuple(item.value for item in enum_type)


def main(argv: Sequence[str] | None = None) -> None:
    scenario_config = parse_scenario_config(argv)
    asyncio.run(run_scenario(
        scenario_config,
        stop_when_complete=True,
        cleanup_all_resources=False,
    ))


if __name__ == "__main__":
    main()
