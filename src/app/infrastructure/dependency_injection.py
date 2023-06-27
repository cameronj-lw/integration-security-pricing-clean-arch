
# core python
from typing import List, Type

# native
from app.domain.events import DomainEvent
from app.infrastructure.event_handlers import KafkaDomainEventHandler
from app.infrastructure.event_publishers import KafkaDomainEventPublisher


def create_kafka_consumer(config_file: str, reset_offset: bool
                        , topics: List[str], event_class: Type[DomainEvent]):
    kafka_consumer = KafkaDomainEventHandler(
        config_file, reset_offset, topics, event_class
    )
    return kafka_consumer

def create_kafka_producer(config_file: str, topics: List[str], flush: bool=True):
    kafka_producer = KafkaDomainEventPublisher(
        config_file, topics, flush
    )

    return kafka_producer

