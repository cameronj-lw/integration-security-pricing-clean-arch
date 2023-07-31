
# core python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Type

# native
from app.domain.events import Event
from app.domain.event_handlers import EventHandler
from app.domain.message_brokers import MessageBroker


@dataclass
class EventSubscriber(ABC):
    message_broker: Type[MessageBroker]
    topics: List[str]
    event_handler: Type[EventHandler]


