
# core python
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class MessageBroker(ABC):
    """ Base class for message queues/brokers """
    config: dict


