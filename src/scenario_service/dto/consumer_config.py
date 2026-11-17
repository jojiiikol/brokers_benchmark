import dataclasses

from src.scenario_service.dto.component_resources import ComponentResources


@dataclasses.dataclass
class ConsumerConfig(ComponentResources):
    count: int
