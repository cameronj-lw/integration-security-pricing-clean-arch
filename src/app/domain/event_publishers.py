
# core python
from abc import ABC, abstractmethod
from typing import List

# native
from app.domain.events import DomainEvent
from app.domain.repositories import MessageBroker


class DomainEventPublisher(ABC):

    @abstractmethod
    def publish(self, *args, **kwargs):
        pass

