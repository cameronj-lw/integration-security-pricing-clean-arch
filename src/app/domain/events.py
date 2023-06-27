
# core python
from abc import ABC
from dataclasses import dataclass

# native
from app.domain.models import (
    Price, PriceBatch, AppraisalBatch, Security, 
    PriceAuditEntry, SecurityWithPrices,
    PriceFeed, PriceFeedWithStatus, PriceSource, PriceType
)


class DomainEvent(ABC):
    pass


@dataclass
class PriceCreatedEvent(DomainEvent):
    price: Price


@dataclass
class PriceBatchCreatedEvent(DomainEvent):
    price_batch: PriceBatch


@dataclass
class AppraisalBatchCreatedEvent(DomainEvent):
    appraisal_batch: AppraisalBatch


@dataclass
class SecurityCreatedEvent(DomainEvent):
    security: Security


@dataclass
class PriceAuditEntryCreatedEvent(DomainEvent):
    entry: PriceAuditEntry


@dataclass
class SecurityWithPricesCreatedEvent(DomainEvent):
    security_with_prices: SecurityWithPrices


@dataclass
class PriceFeedCreatedEvent(DomainEvent):  # TODO: maybe don't need this?
    price_feed: PriceFeed


@dataclass
class PriceFeedWithStatusCreatedEvent(DomainEvent):
    price_feed_with_status: PriceFeedWithStatus


@dataclass
class PriceSourceCreatedEvent(DomainEvent):  # TODO: maybe don't need this?
    price_source: PriceSource


@dataclass
class PriceTypeCreatedEvent(DomainEvent):  # TODO: maybe don't need this?
    price_type: PriceType
    
