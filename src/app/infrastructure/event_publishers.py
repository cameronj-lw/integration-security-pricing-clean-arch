
# core python
from dataclasses import dataclass
import json
import logging
from typing import List, Optional, Tuple, Type, Union

# PyPi
from configparser import ConfigParser
from confluent_kafka import Producer

# native
from app.domain.events import Event
from app.domain.event_publishers import EventPublisher
from app.infrastructure.util.config import AppConfig


@dataclass
class KafkaEventProducer(EventPublisher):
    topics: List[str]

    def __post_init__(self):
        self.config = dict(AppConfig().parser['kafka_broker'])
        self.config.update(AppConfig().parser['kafka_producer'])
        self.producer = Producer(self.config)
    
    def serialize(self, event: Type[self.event_class]) -> Tuple[Optional[str], bytes]:  
        # TODO: does the message always have to be bytes?
        # TODO: should we even allow return value of (key, value)?
        """ Default serialization for kafka. Subclasses may override to meet specific needs."""
        # Convert to dict, then JSON string, then encode to bytes:
        try:
            key = str(event.message_key)
        except AttributeError:
            key = None
        event_dict = event.__dict__
        value = json.dumps(event_dict).encode('utf-8')
        return (key, value)
    
    def callback(self, err, msg):
        if err:
            logging.error('ERROR: Message failed delivery: {}'.format(err))
        else:
            logging.info("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    def publish(self, event: Event, topic: str, flush=True):
        key, value = self.serialize(event)
        for topic in self.topics:
            self.producer.produce(topic, key=key, value=value, on_delivery=self.callback)
            if flush:
                self.producer.flush()

