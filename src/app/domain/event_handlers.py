
# core python
from abc import ABC, abstractmethod
from typing import List

# native
from app.domain.events import DomainEvent


class DomainEventHandler(ABC):
    """ Enforces that all event handlers must handle a DomainEvent """
    @abstractmethod
    def handle(self, event: DomainEvent):
        pass


