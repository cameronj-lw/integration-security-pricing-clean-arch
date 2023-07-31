
# core python
from abc import ABC, abstractmethod
from dataclasses import dataclass

# native
from app.domain.message_brokers import MessageBroker
from app.infrastructure.util.config import AppConfig


class KafkaBroker(MessageBroker):
    def __init__(self):
        super().__init__(config=AppConfig().parser['kafka_broker'])


