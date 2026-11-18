import datetime

from pydantic import BaseModel, ConfigDict, Field

from src.scenario_service.dto.broker_config import BrokerConfig, Brokers
from src.scenario_service.dto.consumer_config import ConsumerConfig
from src.scenario_service.dto.interaction_mode import InteractionMode
from src.scenario_service.dto.producer_config import (
    GuaranteeType,
    LoadType,
    ProducerConfig,
)
from src.scenario_service.dto.scenario_config import ScenarioConfig
from src.scenario_service.service import Scenario


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ComponentResourcesRequest(ApiModel):
    cpu_limit: float = Field(default=1.0, ge=0.001)
    memory_limit_mib: int | None = Field(default=1024, ge=6)


class BrokerConfigRequest(ApiModel):
    broker_type: Brokers


class ConsumerConfigRequest(ComponentResourcesRequest):
    count: int = Field(gt=0)


class ProducerConfigRequest(ComponentResourcesRequest):
    load_type: LoadType
    payload_size: int = Field(gt=0)
    rps: int | None = Field(default=None, gt=0)
    guarantee_type: GuaranteeType = GuaranteeType.AT_MOST_ONCE
    count: int = Field(default=1, gt=0)
    end_rps: int | None = Field(default=None, gt=0)
    step_duration_seconds: float | None = Field(default=None, gt=0)
    max_in_flight: int = Field(default=100, gt=0)


class ScenarioCreateRequest(ApiModel):
    broker: BrokerConfigRequest
    consumer: ConsumerConfigRequest
    producer: ProducerConfigRequest
    interaction_mode: InteractionMode = InteractionMode.WORK_QUEUE
    warmup_seconds: float = Field(default=0, ge=0)
    duration_seconds: float = Field(default=60, gt=0)
    drain_seconds: float = Field(default=10, ge=0)

    def to_domain(self) -> ScenarioConfig:
        return ScenarioConfig(
            broker_config=BrokerConfig(broker_type=self.broker.broker_type),
            consumer_config=ConsumerConfig(**self.consumer.model_dump()),
            producer_config=ProducerConfig(**self.producer.model_dump()),
            interaction_mode=self.interaction_mode,
            warmup_seconds=self.warmup_seconds,
            duration_seconds=self.duration_seconds,
            drain_seconds=self.drain_seconds,
        )


class ScenarioResponse(ApiModel):
    id: str
    name: str
    status: str
    error: str | None
    created_at: datetime.datetime
    started_at: datetime.datetime | None
    warmup_finished_at: datetime.datetime | None
    drain_started_at: datetime.datetime | None
    finished_at: datetime.datetime | None

    @classmethod
    def from_domain(cls, scenario: Scenario) -> "ScenarioResponse":
        return cls(
            id=scenario.id,
            name=scenario.name,
            status=scenario.status.value,
            error=scenario.error,
            created_at=scenario.created_at,
            started_at=scenario.started_at,
            warmup_finished_at=scenario.warmup_finished_at,
            drain_started_at=scenario.drain_started_at,
            finished_at=scenario.finished_at,
        )
