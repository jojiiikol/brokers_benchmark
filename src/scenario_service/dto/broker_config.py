import dataclasses
import enum



class Brokers(enum.Enum):
    RABBITMQ = "rabbit_mq"
    KAFKA = "kafka"
    NATS = "nats"

@dataclasses.dataclass
class BrokerConfig:
    broker_type: Brokers