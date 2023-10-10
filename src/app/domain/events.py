
# core python
from abc import ABC
from dataclasses import dataclass

# native
from app.domain.models import (
    Price, PriceBatch, AppraisalBatch, Security, 
    PriceAuditEntry, SecurityWithPrices,
    PriceFeed, PriceFeedWithStatus, PriceSource, PriceType,
    Position, Portfolio
)


class Event(ABC):
    """ Base class for domain events """


@dataclass
class PriceCreatedEvent(Event):
    price: Price


@dataclass
class PriceBatchCreatedEvent(Event):
    price_batch: PriceBatch


@dataclass
class AppraisalBatchCreatedEvent(Event):
    appraisal_batch: AppraisalBatch


@dataclass
class SecurityCreatedEvent(Event):
    security: Security


@dataclass
class PortfolioCreatedEvent(Event):
    portfolio: Portfolio


@dataclass
class PositionCreatedEvent(Event):
    position: Position


@dataclass
class PositionDeletedEvent(Event):
    position: Position
    
