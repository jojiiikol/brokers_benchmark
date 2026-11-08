import dataclasses
import enum

from src.scenario_service.dto.broker_config import BrokerConfig
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.producer_config import ProducerConfig


@dataclasses.dataclass
class ScenarioConfig:
    broker_config: BrokerConfig
    consumer_config: ConsumerConfig
    producer_config: ProducerConfig
    warmup_seconds: float = 0
    duration_seconds: float = 60

    def __post_init__(self) -> None:
        if self.warmup_seconds < 0:
            raise ValueError("warmup_seconds must be greater than or equal to zero")
        if self.duration_seconds <= 0:
            raise ValueError("duration_seconds must be greater than zero")
