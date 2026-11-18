import asyncio

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


def main() -> None:
    scenario = ScenarioConfig(
        broker_config=BrokerConfig(broker_type=Brokers.NATS_PULL),
        consumer_config=ConsumerConfig(
            count=1,
            cpu_limit=1,
            memory_limit_mib=1024,
        ),
        producer_config=ProducerConfig(
            load_type=LoadType.MAX_THROUGHPUT,
            payload_size=16,
            rps=None,
            guarantee_type=GuaranteeType.AT_LEAST_ONCE,
            count=1,
            max_in_flight=64,
            cpu_limit=1,
            memory_limit_mib=1024,
        ),
        warmup_seconds=15,
        duration_seconds=60,
        interaction_mode=InteractionMode.WORK_QUEUE,
    )
    asyncio.run(run_scenario(
        scenario,
        stop_when_complete=False,
        cleanup_all_resources=True,
    ))


if __name__ == "__main__":
    main()
