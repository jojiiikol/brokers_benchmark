
from src.scenario_service.creator.base.abstract_service import BaseServiceCreator
from src.scenario_service.dto.producer_config import ProducerConfig


class BaseProducerCreator(BaseServiceCreator):
    async def configurate(
            self,
            scenario_id: str,
            config: ProducerConfig,
            *args,
            **kwargs
    ):
        docker_args = {
            'rps', 'payload_size'
        }

        for field in config.__dict__:
            if field in docker_args:
                if getattr(config, field) is None:
                    continue

                config.docker_args.extend(
                    [f"--{field}", str(getattr(config, field))]
                )

        return await super().configurate(scenario_id, config, *args, **kwargs)
