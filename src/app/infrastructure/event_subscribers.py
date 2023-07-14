
# core python
from abc import abstractmethod
from dataclasses import dataclass
import datetime
import json
import logging
import time
from typing import List, Type


# pypi
from confluent_kafka import Consumer, OFFSET_BEGINNING, OFFSET_END

# native
from app.domain.events import Event, AppraisalBatchCreatedEvent, PriceBatchCreatedEvent, SecurityCreatedEvent
from app.domain.event_handlers import EventHandler
from app.domain.event_subscribers import EventSubscriber
from app.domain.message_brokers import MessageBroker
from app.domain.models import Security, AppraisalBatch, PriceBatch, PriceSource

from app.infrastructure.message_brokers import KafkaBroker
from app.infrastructure.util.config import AppConfig



@dataclass
class KafkaEventConsumer(EventSubscriber):
    topics: List[str]

    def __post_init__(self):
        self.config = dict(AppConfig().parser['kafka_broker'])
        self.config.update(AppConfig().parser['kafka_consumer'])
        self.consumer = Consumer(self.config)
        # self.consumer.subscribe(self.topics, on_assign=self.on_assign)
        # TODO: remove above when not needed
    
    # TODO: delete once not needed, i.e. once confirmed using dataclass with post_init is desired
    # def __init__(self, config_file: str, reset_offset: bool, topics: List[str], event_class: Type[Event]):
    #     # super().__init__()  # TODO: remove if not needed, i.e. if not using EventHandler as base class
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

    def consume(self, reset_offset: bool=False):
        self.reset_offset = reset_offset
        self.consumer.subscribe(self.topics, on_assign=self.on_assign)
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
                    logging.info(f"Consuming message: {msg.value()}")
                    event = self.deserialize(msg.value())
                    logging.info(f"Handling {event}")
                    self.event_handler.handle(event)  
                    logging.info(f"Done handling {event}")
                    # self.consumer.commit(message=msg, asynchronous=False)
                    # logging.info("Done committing offset")
                # time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self.consumer.close()


    def on_assign(self, consumer, partitions):
        # TODO: confirm this works as class method
        if self.reset_offset:
            for p in partitions:
                logging.info(f"Resetting offset for {p}")
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

    @abstractmethod
    def deserialize(self, message_value: bytes) -> Event:  # TODO: does the message always have to be bytes?
        """ Subclasses of KafkaEventConsumer must implement a deserialize method """
        
    def __del__(self):
        self.consumer.close()


class KafkaCoreDBSecurityCreatedEventConsumer(KafkaEventConsumer):
    def __init__(self, event_handler: EventHandler):
        """ Creates a KafkaEventConsumer to consumer new/changed coredb securities with the provided event handler """
        super().__init__(message_broker=KafkaBroker(), event_class=SecurityCreatedEvent
                , event_handler=event_handler, topics=[AppConfig().parser.get('kafka_topics', 'coredb_security')])

    def deserialize(self, message_value: bytes) -> SecurityCreatedEvent:
        event_dict = json.loads(message_value.decode('utf-8'))
        lw_id = event_dict['lw_id']
        attributes = {k:event_dict[k] for k in event_dict if k != 'lw_id'}

        # Message could contain a modified_at, in seconds since epoch
        if 'modified_at' in attributes:
            if isinstance(attributes['modified_at'], int):
                attributes['modified_at'] = datetime.datetime.fromtimestamp(attributes['modified_at'] / 1000.0)
            attributes['modified_at'] = attributes['modified_at'].isoformat()

        sec = Security(lw_id=lw_id, attributes=attributes)
        event = self.event_class(sec)
        return event


class KafkaCoreDBAppraisalBatchCreatedEventConsumer(KafkaEventConsumer):
    def __init__(self, event_handler: EventHandler):
        """ Creates a KafkaEventConsumer to consumer new/changed coredb appraisal batches with the provided event handler """
        super().__init__(message_broker=KafkaBroker(), event_class=AppraisalBatchCreatedEvent
                , event_handler=event_handler, topics=[AppConfig().parser.get('kafka_topics', 'coredb_appraisal_batch')])

    def deserialize(self, message_value: bytes) -> AppraisalBatchCreatedEvent:
        # Get dict from Kafka message
        event_dict = json.loads(message_value.decode('utf-8'))
        event_dict = {k.lower(): v for k, v in event_dict.items()}
        
        # Populate default for portfolios ... note this should not be long-term
        if 'portfolios' not in event_dict:
            event_dict['portfolios'] = '@LW_OpenandMeasurementandTest'  # TODO: should this be an assumed default?
        
        # Convert "days since epoch" to date
        date = (datetime.datetime(year=1970, month=1, day=1) + datetime.timedelta(days=event_dict['data_date'])).date()

        # Create batch, then event and return it
        batch = AppraisalBatch(portfolios=event_dict['portfolios'], data_date=date)
        event = self.event_class(batch)
        return event


class KafkaCoreDBPriceBatchCreatedEventConsumer(KafkaEventConsumer):
    def __init__(self, event_handler: EventHandler):
        """ Creates a KafkaEventConsumer to consumer new/changed coredb price batches with the provided event handler """
        super().__init__(message_broker=KafkaBroker(), event_class=PriceBatchCreatedEvent
                , event_handler=event_handler, topics=[AppConfig().parser.get('kafka_topics', 'coredb_price_batch')])

    def deserialize(self, message_value: bytes) -> PriceBatchCreatedEvent:
        event_dict = json.loads(message_value.decode('utf-8'))
        event_dict = {k.lower(): v for k, v in event_dict.items()}
        date = (datetime.datetime(year=1970, month=1, day=1) + datetime.timedelta(days=event_dict['data_date'])).date()
        batch = PriceBatch(source=PriceSource(event_dict['source']), data_date=date)
        event = self.event_class(batch)
        return event



