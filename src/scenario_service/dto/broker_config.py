import dataclasses
import enum



class Brokers(enum.Enum):
    RABBITMQ = "rabbit_mq"
    KAFKA = "kafka"
    NATS = "nats"

@dataclasses.dataclass
class BrokerConnection:
    broker_type: Brokers
    host: str = "localhost"
    port: int = 5672
    username: str = "guest"
    password: str = "guest"

@dataclasses.dataclass
class BrokerConfig:
    broker_type: Brokers