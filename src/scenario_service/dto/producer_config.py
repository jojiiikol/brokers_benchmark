import dataclasses
import enum
from typing import Any

from src.scenario_service.dto.component_resources import ComponentResources


class LoadType(enum.Enum):
    CONSTANT = "constant"
    LINEAR = "linear"
    MAX_THROUGHPUT = "max_throughput"
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
class ProducerConfig(ComponentResources):
    load_type: LoadType
    payload_size: int
    rps: int | None
    guarantee_type: GuaranteeType
    count: int
    end_rps: int | None = None
    step_duration_seconds: float | None = None
    max_in_flight: int = 100

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.payload_size <= 0:
            raise ValueError("payload_size must be greater than zero")
        if self.count <= 0:
            raise ValueError("count must be greater than zero")
        if self.max_in_flight <= 0:
            raise ValueError("max_in_flight must be greater than zero")
        if self.guarantee_type is GuaranteeType.EXACTLY_ONCE:
            raise ValueError(
                "exactly_once is not part of the benchmark: it requires "
                "application-level transactions and deduplication."
            )

        if self.load_type is LoadType.MAX_THROUGHPUT:
            if any(value is not None for value in (
                self.rps, self.end_rps, self.step_duration_seconds
            )):
                raise ValueError("max_throughput load does not accept a target rps")
            return

        if self.rps is None or self.rps <= 0:
            raise ValueError("rps must be greater than zero")

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



