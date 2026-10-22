import dataclasses


@dataclasses.dataclass
class BrokerConfig:
    qos: int


class RabbitMQConfig(BrokerConfig):
    prefetch_count: int
