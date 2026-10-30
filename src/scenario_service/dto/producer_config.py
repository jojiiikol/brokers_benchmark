import dataclasses
import enum
from typing import Any

from .broker_config import BrokerConnection


class LoadType(enum.Enum):
    linear = "linear"
    exponential = "exponential"
    spike = "spike"

class GuaranteeType(enum.Enum):
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


@dataclasses.dataclass
class ProducerConfig:
    load_type: LoadType
    message_size: int
    speed: int
    guarantee_type: GuaranteeType
    count: int
    docker_args: list[str]
    broker_connection: BrokerConnection



