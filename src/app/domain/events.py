
# core python
from abc import ABC
from dataclasses import dataclass

# native
from app.domain.models import (
    Price, PriceBatch, AppraisalBatch, Security, 
    PriceAuditEntry, SecurityWithPrices,
    PriceFeed, PriceFeedWithStatus, PriceSource, PriceType
)


class Event(ABC):
    pass


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
class PriceAuditEntryCreatedEvent(Event):
    entry: PriceAuditEntry


@dataclass
class SecurityWithPricesCreatedEvent(Event):
    security_with_prices: SecurityWithPrices


@dataclass
class PriceFeedCreatedEvent(Event):  # TODO: maybe don't need this?
    price_feed: PriceFeed


@dataclass
class PriceFeedWithStatusCreatedEvent(Event):
    price_feed_with_status: PriceFeedWithStatus


@dataclass
class PriceSourceCreatedEvent(Event):  # TODO: maybe don't need this?
    price_source: PriceSource


@dataclass
class PriceTypeCreatedEvent(Event):  # TODO: maybe don't need this?
    price_type: PriceType
    
