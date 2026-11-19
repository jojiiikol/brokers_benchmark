"""Public experiment specification; all generated runs use the scenario DTO."""

import hashlib
import json
import random
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.api.models import ConsumerConfigRequest, ProducerConfigRequest, ScenarioCreateRequest
from src.scenario_service.dto.broker_config import Brokers
from src.scenario_service.dto.interaction_mode import InteractionMode


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class ExperimentType(str, Enum):
    PRODUCERS = "producer_scaling"
    CONSUMERS = "consumer_scaling"
    SIZE = "message_size"
    PUB_SUB = "pub_sub"
    RAMP = "load_ramp"


class ScenarioTemplate(Model):
    producer: ProducerConfigRequest
    consumer: ConsumerConfigRequest
    interaction_mode: InteractionMode = InteractionMode.WORK_QUEUE
    warmup_seconds: float = Field(default=30, ge=0)
    duration_seconds: float = Field(default=120, gt=0)
    drain_seconds: float = Field(default=10, ge=0)


class Ramp(Model):
    start_rps: int = Field(gt=0)
    end_rps: int = Field(gt=0)
    step_duration_seconds: float = Field(gt=0)


class ExperimentSpecification(Model):
    name: str = Field(min_length=1, max_length=200)
    experiment_type: ExperimentType
    brokers: list[Brokers] = Field(min_length=2)
    values: list[int] | None = None
    ramp: Ramp | None = None
    repetitions: int = Field(gt=0)
    seed: int = Field(default=42, ge=0)
    scenario: ScenarioTemplate

    @model_validator(mode="after")
    def validate_plan(self):
        if self.scenario.producer.load_type.value == "spike":
            raise ValueError("spike load is not implemented")
        if len(set(self.brokers)) != len(self.brokers):
            raise ValueError("brokers must be unique")
        if self.experiment_type is ExperimentType.RAMP:
            if self.values is not None or self.ramp is None:
                raise ValueError("load_ramp requires ramp instead of values")
        elif (self.ramp is not None or not self.values or
              any(type(v) is not int or v <= 0 for v in self.values) or
              len(set(self.values)) != len(self.values)):
            raise ValueError("values must contain unique positive integers; ramp is only for load_ramp")
        expected_mode = (InteractionMode.PUB_SUB if self.experiment_type is ExperimentType.PUB_SUB
                         else InteractionMode.WORK_QUEUE)
        if self.scenario.interaction_mode is not expected_mode:
            raise ValueError(f"{self.experiment_type.value} requires {expected_mode.value}")
        if self.experiment_type is not ExperimentType.RAMP and self.scenario.producer.load_type.value != "constant":
            raise ValueError("scaling and message-size experiments require constant load")
        for broker in self.brokers:
            for value in self.values or [None]:
                self.scenario_request(broker, value).to_domain()
        return self

    def scenario_request(self, broker: Brokers, value: int | None) -> ScenarioCreateRequest:
        data = self.scenario.model_dump(mode="json")
        data["broker"] = {"broker_type": broker.value}
        if self.experiment_type is ExperimentType.RAMP:
            ramp = self.ramp
            assert ramp is not None
            data["producer"].update(load_type="linear", rps=ramp.start_rps,
                                    end_rps=ramp.end_rps,
                                    step_duration_seconds=ramp.step_duration_seconds)
        else:
            role, field = {
                ExperimentType.PRODUCERS: ("producer", "count"),
                ExperimentType.CONSUMERS: ("consumer", "count"),
                ExperimentType.SIZE: ("producer", "payload_size"),
                ExperimentType.PUB_SUB: ("consumer", "count"),
            }[self.experiment_type]
            data[role][field] = value
        return ScenarioCreateRequest.model_validate(data)


def build_plan(spec: ExperimentSpecification) -> dict:
    configurations = []
    for broker in spec.brokers:
        for value in spec.values or [None]:
            config = spec.scenario_request(broker, value).model_dump(mode="json")
            digest = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()[:24]
            configurations.append(dict(configuration_id=digest, broker=broker.value,
                                       value=value, scenario=config))
    rng = random.Random(spec.seed)
    runs = []
    for repetition in range(1, spec.repetitions + 1):
        block = list(configurations)
        rng.shuffle(block)
        for config in block:
            runs.append(dict(run_id=f"{config['configuration_id']}-{repetition}",
                             configuration_id=config["configuration_id"], repetition=repetition))
    return dict(schema_version=1, configurations=configurations, runs=runs,
                total_runs=len(runs), order="randomized_blocks", seed=spec.seed)
