
# core python
from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
import json
import logging
import os
from typing import List, Optional, Tuple, Type, Union

# PyPi
from configparser import ConfigParser
from confluent_kafka import Producer

# native
from app.domain.events import (Event
    , PortfolioCreatedEvent
    , PositionCreatedEvent, PositionDeletedEvent
)
from app.domain.event_publishers import EventPublisher

from app.infrastructure.message_brokers import KafkaBroker
from app.infrastructure.util.config import AppConfig
from app.infrastructure.util.date import format_time


class KafkaEventProducer(EventPublisher):
    def __init__(self, topics: List[str]):
        super().__init__(message_broker=KafkaBroker(), topics=topics)
        self.config = dict(AppConfig().parser['kafka_broker'])
        self.config.update(AppConfig().parser['kafka_producer'])
        self.producer = Producer(self.config)
    
    @abstractmethod
    def serialize(self, event: Type[Event]) -> Tuple[Optional[str], bytes]:  
        # TODO: does the message always have to be bytes?
        """ Subclasses of KafkaEventConsumer must implement a deserialize method """
        # TODO_CLEANUP: remove below once not needed, i.e. once confirmed we want this
        # method as an abstractmethod
        # TODO: should we even allow return value of (key, value)?
        """ Default serialization for kafka. Subclasses may override to meet specific needs."""
        # Convert to dict, then JSON string, then encode to bytes:
        # try:
        #     key = str(event.message_key)
        # except AttributeError:
        #     key = None
        # event_dict = event.__dict__
        # value = json.dumps(event_dict).encode('utf-8')
        # return (key, value)
    
    def callback(self, err, msg):
        if err:
            logging.error('ERROR: Message failed delivery: {}'.format(err))
        else:
            logging.info("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    def publish(self, event: Event, flush=True):
        key, value = self.serialize(event)
        for topic in self.topics:
            self.producer.produce(topic, key=key, value=value, on_delivery=self.callback)
            if flush:
                self.producer.flush()


class KafkaCoreDBPortfolioCreatedEventProducer(KafkaEventProducer):
    def __init__(self):
        super().__init__(topics=[AppConfig().get('kafka_topics', 'coredb_portfolio')])
        # TODO: better schema management? Schema registry? 
        self.schema = {
            "name": "coredb_portfolio",
            "type": "struct",
            "fields": [
                {
                    "field": "pms_portfolio_id",
                    "type": "int32"
                },
                {
                    "field": "portfolio_code",
                    "type": "string",
                    "optional": True
                },
                {
                    "field": "portfolio_type",
                    "type": "string",
                    "optional": True
                },
                {
                    "field": "modified_at",
                    "type": "string",
                    "optional": True
                },
                {
                    "field": "modified_by",
                    "type": "string",
                    "optional": True
                },
            ]
        }
    
    def serialize(self, event: PortfolioCreatedEvent) -> Tuple[Optional[str], bytes]:
        
        # Build dict in required format for the CoreDB portfolio
        portfolio = event.portfolio
        coredb_portfolio_dict = {
            'pms_portfolio_id'  : portfolio.attributes['pms_portfolio_id'],
            'portfolio_code'	: portfolio.portfolio_code,
            'portfolio_type'	: portfolio.attributes['portfolio_type'],
            'modified_at'		: format_time(datetime.datetime.now()),
            'modified_by'		: os.path.basename(__file__)
        }

        # Return key and value
        key = str(coredb_portfolio_dict['portfolio_code'])
        value_dict = {
            "schema": self.schema,
            "payload": coredb_portfolio_dict
        }
        value = json.dumps(value_dict).encode('utf-8')
        logging.debug(f'Derived key and value portfolio: {portfolio}\n{key}\n{value}')
        return (key, value)


class KafkaCoreDBPositionEventProducer(KafkaEventProducer):
    def __init__(self):
        super().__init__(topics=[AppConfig().get('kafka_topics', 'coredb_position')])
        # TODO: better schema management? Schema registry? 
        self.schema = {
            "name": "coredb_position",
            "type": "struct",
            "fields": [
                {
                    "field": "pms_position_id",
                    "type": "int32"
                },
                {
                    "field": "pms_portfolio_id",
                    "type": "int32"
                },
                {
                    "field": "pms_security_id",
                    "type": "int32"
                },
                {
                    "field": "is_short",
                    "type": "boolean"
                },
                {
                    "field": "quantity",
                    "type": "float"
                },
                {
                    "field": "modified_at",
                    "type": "string",
                    "optional": True
                },
                {
                    "field": "modified_by",
                    "type": "string",
                    "optional": True
                },
                {
                    "field": "is_deleted",
                    "type": "boolean",
                    "optional": True
                },
            ]
        }
    
    def serialize(self, event: Union[PositionCreatedEvent, PositionDeletedEvent]
            ) -> Tuple[Optional[str], bytes]:
        
        # Build dict in required format for the CoreDB position
        position = event.position
        coredb_position_dict = {
            'pms_position_id'	: position.attributes['pms_position_id'],
            'pms_portfolio_id'  : position.portfolio.attributes['pms_portfolio_id'],
            'pms_security_id'	: position.security.attributes['pms_security_id'],
            'is_short'			: position.is_short,
            'quantity'			: position.quantity,
            'modified_at'		: format_time(datetime.datetime.now()),
            'modified_by'		: os.path.basename(__file__),
            'is_deleted'        : True if isinstance(event, PositionDeletedEvent) else False
        }

        # Return key and value
        key = str(coredb_position_dict['pms_position_id'])
        value_dict = {
            "schema": self.schema,
            "payload": coredb_position_dict
        }
        value = json.dumps(value_dict).encode('utf-8')
        logging.debug(f'Derived key and value position: {position}\n{key}\n{value}')
        return (key, value)





