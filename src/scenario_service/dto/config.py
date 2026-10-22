import dataclasses
import enum


class Brokers(enum.Enum):
    RABBITMQ = "rabbit_mq"
    KAFKA = "kafka"
    NATS = "nats"

class LoadType(enum.Enum):
    linear = "linear"
    exponential = "exponential"
    spike = "spike"

class GuaranteeType(enum.Enum):
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"

@dataclasses.dataclass
class ScenarioConfig:
    broker_type: Brokers

    consumers_count: int
    producers_count: int

    load_type: LoadType
    guarantee_type: str
    intensity: int

    message_size: int