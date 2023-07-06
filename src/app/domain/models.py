
# core python
from dataclasses import dataclass, field
import datetime
from typing import List, Type


@dataclass
class PriceSource:
    name: str  # TODO: enforce that the name must be in the hierarchy?
    # Top in the hierarchy is at the top. TODO: add others not required for pricing revamp?
    # HIERARCHY: List[str] = [
    #     'OVERRIDE'
    #     ,'MANUAL'
    #     ,'FUNDRUN'
    #     ,'FTSE'
    #     ,'MARKIT'
    #     ,'BLOOMBERG'
    #     ,'RBC'
    # ]

    # def __gt__(self, other):
    #     if self.name not in cls.HIERARCHY:
    #         return False
    #     try:
    #         return (cls.HIERARCHY.index(self.name) < cls.HIERARCHY.index(other.name))
    #     except ValueError as e:
    #         # TODO: specific exception to support logging? Note logging should not happen in domain layer
    #         return True


@dataclass
class PriceType:
    name: str


@dataclass 
class Security:
    lw_id: str
    attributes: dict = field(default_factory=dict)  # TODO: should this class require specific attributes?

    def to_dict(self):
        res = {'lw_id': self.lw_id}
        res.update(self.attributes)
        return res


@dataclass
class Price:
    security: Security
    source: PriceSource
    data_date: datetime.date
    modified_at: datetime.datetime
    type_: PriceType
    value: float

    def to_dict(self):
        res = {'lw_id': self.security.lw_id
            , 'data_date': self.data_date.isoformat()
            , 'source': self.source.name
            , 'modified_at': self.modified_at.isoformat()
            , self.type_.name: self.value
        }
        return res


@dataclass
class SecurityWithPrices:
    security: Security
    data_date: datetime.date
    prices: List[Price]

    def to_dict(self):
        res = self.security.to_dict()
        res['data_date'] = self.data_date.isoformat()
        res['prices'] = [px.to_dict() for px in self.prices]
        return res


@dataclass
class PriceBatch:
    source: PriceSource
    data_date: datetime.date


@dataclass
class AppraisalBatch:
    portfolios: str
    data_date: datetime.date


@dataclass
class Portfolio:
    portfolio_code: str


@dataclass
class Position:
    portfolio: Portfolio
    data_date: datetime.date
    security: Security
    prices: List[Price]  # TODO: only allow one price per price type?


@dataclass
class PriceFeed:
    name: str


@dataclass
class PriceFeedWithStatus:
    feed: PriceFeed
    data_date: datetime.date
    status: str = None
    status_ts: datetime.datetime = None
    price_feed_class: Type[PriceFeed] = PriceFeed


@dataclass
class PriceAuditEntry:
    data_date: datetime.date
    security: Security
    reason: str
    comment: str
    before: List[Price]
    after: List[Price]
    modified_by: str
    modified_at: datetime.datetime



