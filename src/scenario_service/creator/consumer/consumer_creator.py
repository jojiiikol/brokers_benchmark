from src.scenario_service.creator.base.abstract_consumer import BaseConsumerCreator


class ConsumerCreator(BaseConsumerCreator):
    def __init__(
            self,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        pass

    def configurate(self):
        pass