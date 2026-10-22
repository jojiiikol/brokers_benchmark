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