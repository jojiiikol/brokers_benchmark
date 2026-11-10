import dataclasses
import enum
from typing import Any

from .broker_config import BrokerConnection


class LoadType(enum.Enum):
    CONSTANT = "constant"
    LINEAR = "linear"
    SPIKE = "spike"

    # Compatibility aliases for existing callers. New code must use uppercase
    # names so a constant profile is not confused with a linear ramp.
    constant = CONSTANT
    linear = LINEAR
    spike = SPIKE

class GuaranteeType(enum.Enum):
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


@dataclasses.dataclass
class ProducerConfig:
    load_type: LoadType
    payload_size: int
    rps: int
    guarantee_type: GuaranteeType
    count: int
    docker_args: list[str]
    broker_connection: BrokerConnection
    end_rps: int | None = None
    step_duration_seconds: float | None = None

    def __post_init__(self) -> None:
        if self.rps <= 0:
            raise ValueError("rps must be greater than zero")
        if self.payload_size <= 0:
            raise ValueError("payload_size must be greater than zero")
        if self.count <= 0:
            raise ValueError("count must be greater than zero")

        if self.load_type is LoadType.CONSTANT:
            if self.end_rps is not None or self.step_duration_seconds is not None:
                raise ValueError("constant load does not accept ramp parameters")
            return

        if self.load_type is LoadType.LINEAR:
            if self.end_rps is None or self.step_duration_seconds is None:
                raise ValueError(
                    "linear load requires end_rps and step_duration_seconds"
                )
            if self.end_rps <= self.rps:
                raise ValueError("linear load end_rps must be greater than rps")
            if self.step_duration_seconds <= 0:
                raise ValueError(
                    "linear load step_duration_seconds must be greater than zero"
                )
            return

        raise ValueError(f"Load type {self.load_type.value} is not implemented")



