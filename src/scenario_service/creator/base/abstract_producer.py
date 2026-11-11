
import dataclasses

from src.scenario_service.creator.base.abstract_service import BaseServiceCreator
from src.scenario_service.dto.producer_config import LoadType, ProducerConfig


class BaseProducerCreator(BaseServiceCreator):
    async def configurate(
            self,
            scenario_id: str,
            config: ProducerConfig,
            instance_index: int | None = None,
            *args,
            **kwargs
    ):
        docker_args = [
            *config.docker_args,
            "--load-type",
            config.load_type.value,
            "--guarantee-type",
            config.guarantee_type.value,
            "--max-in-flight",
            str(config.max_in_flight),
        ]
        if config.load_type is LoadType.CONSTANT:
            docker_args.extend(("--rps", str(config.rps)))
        elif config.load_type is LoadType.LINEAR:
            docker_args.extend(
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
        docker_args.extend(("--payload_size", str(config.payload_size)))
        container_config = dataclasses.replace(config, docker_args=docker_args)
        return await super().configurate(
            scenario_id,
            container_config,
            instance_index=instance_index,
            *args,
            **kwargs,
        )
