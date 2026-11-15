from src.scenario_service.creator.producer.producer_creator import ProducerCreator


class _NatsProducerCreator(ProducerCreator):
    def component_broker_type(self, broker_type) -> str:
        return "nats"


class NatsPullProducerCreator(_NatsProducerCreator):
    pass


class NatsPushProducerCreator(_NatsProducerCreator):
    pass
