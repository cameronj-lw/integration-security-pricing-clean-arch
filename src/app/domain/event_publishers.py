
# core python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Type

# native
from app.domain.events import Event
from app.domain.message_brokers import MessageBroker


@dataclass
class EventPublisher(ABC):
    message_broker: Type[MessageBroker]
    event_class: Type[Event]

