import dataclasses
import enum
import math

from src.scenario_service.dto.broker_config import (
    BrokerConfig,
    Brokers,
    MAX_KAFKA_WORK_QUEUE_PARTITIONS,
)
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.producer_config import ProducerConfig
from src.scenario_service.dto.producer_config import LoadType
from src.scenario_service.dto.interaction_mode import InteractionMode


@dataclasses.dataclass
class ScenarioConfig:
    broker_config: BrokerConfig
    consumer_config: ConsumerConfig
    producer_config: ProducerConfig
    interaction_mode: InteractionMode = InteractionMode.WORK_QUEUE
    warmup_seconds: float = 0
    duration_seconds: float = 60
    drain_seconds: float = 10

    def __post_init__(self) -> None:
        if self.warmup_seconds < 0:
            raise ValueError("warmup_seconds must be greater than or equal to zero")
        if self.duration_seconds <= 0:
            raise ValueError("duration_seconds must be greater than zero")
        if not math.isfinite(self.drain_seconds) or self.drain_seconds < 0:
            raise ValueError("drain_seconds must be finite and non-negative")
        if (
            self.broker_config.broker_type is Brokers.KAFKA
            and self.interaction_mode is InteractionMode.WORK_QUEUE
            and self.consumer_config.count > MAX_KAFKA_WORK_QUEUE_PARTITIONS
        ):
            raise ValueError(
                "Kafka work_queue supports at most "
                f"{MAX_KAFKA_WORK_QUEUE_PARTITIONS} consumers because each "
                "consumer requires one partition"
            )
        producer = self.producer_config
        if producer.load_type is LoadType.LINEAR:
            step = producer.step_duration_seconds
            assert step is not None
            steps, remainder = divmod(self.duration_seconds, step)
            if not math.isclose(remainder, 0.0, abs_tol=1e-9):
                raise ValueError(
                    "duration_seconds must be divisible by "
                    "step_duration_seconds for linear load"
                )
            if steps < 2:
                raise ValueError(
                    "linear load requires at least two load steps during "
                    "the measurement phase"
                )
