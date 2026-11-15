
import dataclasses

from src.scenario_service.creator.base.abstract_service import BaseServiceCreator
from src.scenario_service.dto.producer_config import LoadType, ProducerConfig
from src.scenario_service.dto.broker_config import Brokers


class BaseProducerCreator(BaseServiceCreator):
    async def configurate(
            self,
            scenario_id: str,
            config: ProducerConfig,
            broker_type: Brokers,
            docker_args: list[str],
            instance_index: int | None = None,
            *args,
            **kwargs
    ):
        launch_args = [
            *docker_args,
            "--load-type",
            config.load_type.value,
            "--guarantee-type",
            config.guarantee_type.value,
            "--max-in-flight",
            str(config.max_in_flight),
        ]
        if config.load_type is LoadType.CONSTANT:
            launch_args.extend(("--rps", str(config.rps)))
        elif config.load_type is LoadType.LINEAR:
            launch_args.extend(
                (
                    "--start-rps",
                    str(config.rps),
                    "--end-rps",
                    str(config.end_rps),
                    "--step-duration-seconds",
                    str(config.step_duration_seconds),
                )
            )
        else:
            raise ValueError(f"Unsupported load type {config.load_type.value}")
        launch_args.extend(("--payload_size", str(config.payload_size)))
        return await super().configurate(
            scenario_id,
            config,
            broker_type=broker_type,
            docker_args=launch_args,
            instance_index=instance_index,
            *args,
            **kwargs,
        )
