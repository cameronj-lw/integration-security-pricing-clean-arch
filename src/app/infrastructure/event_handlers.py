
# core python
from dataclasses import dataclass
import json
import logging
from typing import List, Type


# pypi
from confluent_kafka import Consumer, OFFSET_BEGINNING, OFFSET_END

# native
from app.domain.event_handlers import DomainEventHandler  # TODO: maybe don;t need this base class
from app.domain.events import DomainEvent
from app.infrastructure.util.config import AppConfig



@dataclass
class KafkaDomainEventHandler():
    event_class: Type[DomainEvent]

    def __post_init__(self):
        self.config = dict(AppConfig().parser['default'])
        self.config.update(AppConfig().parser['consumer'])
        self.consumer = Consumer(self.config)
        # self.consumer.subscribe(self.topics, on_assign=self.on_assign)
        # TODO: remove above when not needed
    
    # TODO: delete once not needed, i.e. once confirmed using dataclass with post_init is desired
    # def __init__(self, config_file: str, reset_offset: bool, topics: List[str], event_class: Type[DomainEvent]):
    #     # super().__init__()  # TODO: remove if not needed, i.e. if not using DomainEventHandler as base class
    #     self.config_file = config_file
    #     self.reset_offset = reset_offset
    #     self.topics = topics
    #     # Create the Consumer based on config file
    #     self.config_parser = ConfigParser()
    #     self.config_parser.read_file(self.config_file)
    #     self.config = dict(self.config_parser['default'])
    #     self.config.update(self.config_parser['consumer'])
    #     self.consumer.subscribe(self.topics, on_assign=reset_offset)
    #     try:  # TODO: should this not belong inside init method?
    #         while True:
    #             msg = self.consumer.poll(1.0)
    #     except KeyboardInterrupt:
    #         pass
    #     finally:
    #         # Leave group and commit final offsets
    #         self.consumer.close()

    def register_event_handler(self, event_handler_instance):
        self.event_handler = event_handler_instance

    def consume(self, topic: str, reset_offset: bool=False):
        self.reset_offset = reset_offset
        self.consumer.subscribe([topic], on_assign=self.on_assign)
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    logging.debug("Waiting...")
                elif msg.error():
                    logging.error("ERROR: %s".format(msg.error()))
                    # TODO: raise exception?
                elif msg.value() is not None:
                    event = self.deserialize(msg.value())
                    self.event_handler.handle_event(event)        
        except AttributeError:
            logging.error(f"Must register_event_handler before consuming!")
            pass  # TODO: different exception?
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self.consumer.close()


    def on_assign(self, consumer, partitions):
        # TODO: confirm this works as class method
        if self.reset_offset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # TODO: remove when not needed
    # def get_single_event(self) -> self.event_class:
    #     while True:
    #         msg = self.consumer.poll(1.0)
    #         if msg is None:
    #             # Initial message consumption may take up to
    #             # `session.timeout.ms` for the consumer group to
    #             # rebalance and start consuming
    #             logging.debug("Waiting...")
    #         elif msg.error():
    #             logging.error("ERROR: %s".format(msg.error()))
    #         elif msg.value() is not None:
    #             # Extract the (optional) key and value, transform, and produce to coredb topic.
    #             return self.deserialize(msg.value())

    def deserialize(self, message_value: bytes):  # TODO: does the message always have to be bytes?
        """ Default deserialization for kafka. Subclasses may override to meet specific needs."""
        # Convert bytes to JSON string, then dict, and then create the DomainEvent to return:
        event_dict = json.loads(message_value.decode('utf-8'))
        event = self.event_class(event_dict)
        return event
        
    def __del__(self):
        self.consumer.close()


