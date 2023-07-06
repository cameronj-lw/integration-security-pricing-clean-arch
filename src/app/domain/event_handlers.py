
# core python
from abc import ABC, abstractmethod
from typing import List

# native
from app.domain.events import Event


class EventHandler(ABC):
    
    @abstractmethod
    def handle(self, event: Event):
        """ Event handlers must handle a Event """


