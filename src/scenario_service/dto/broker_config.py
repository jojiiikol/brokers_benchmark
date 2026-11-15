import dataclasses
import enum


MAX_KAFKA_WORK_QUEUE_PARTITIONS = 16


class Brokers(enum.Enum):
    RABBITMQ = "rabbit_mq"
    KAFKA = "kafka"
    NATS_PULL = "nats_pull"
    NATS_PUSH = "nats_push"

@dataclasses.dataclass
class BrokerConfig:
    broker_type: Brokers
