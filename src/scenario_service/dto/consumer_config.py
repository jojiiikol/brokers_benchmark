import dataclasses

from .broker_config import BrokerConnection


@dataclasses.dataclass
class ConsumerConfig:
    count: int
    docker_args: list[str]
    broker_connection: BrokerConnection