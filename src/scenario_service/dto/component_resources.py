import dataclasses
import math


@dataclasses.dataclass(kw_only=True)
class ComponentResources:
    cpu_limit: float = 1.0
    memory_limit_mib: int | None = 1024

    def __post_init__(self) -> None:
        if not math.isfinite(self.cpu_limit) or self.cpu_limit < 0.001:
            raise ValueError("cpu_limit must be finite and at least 0.001 CPU")
        if self.memory_limit_mib is not None and (
            not isinstance(self.memory_limit_mib, int) or self.memory_limit_mib < 6
        ):
            raise ValueError("memory_limit_mib must be an integer of at least 6 MiB")
