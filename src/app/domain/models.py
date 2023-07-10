
# core python
from dataclasses import dataclass, field
import datetime
import logging
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

    def __gt__(self, other):
        logging.error(f'Inside __gt__! {self} {other}')
        HIERARCHY = [
            'OVERRIDE'
            ,'MANUAL'
            ,'FUNDRUN'
            ,'FTSE'
            ,'MARKIT'
            ,'BLOOMBERG'
            ,'RBC'
        ]
        if self.name not in HIERARCHY:
            return False
        elif other.name not in HIERARCHY:
            return True
        logging.info(f'Comparing {self.name} {HIERARCHY.index(self.name)} vs {other.name} {HIERARCHY.index(other.name)}')
        return (HIERARCHY.index(self.name) < HIERARCHY.index(other.name))


@dataclass
class PriceType:
    name: str


@dataclass 
class Security:
    lw_id: str
    attributes: dict = field(default_factory=dict)  # TODO: should this class require specific attributes?

    def to_dict(self):
        """ Export an instance to dict format """
        res = {'lw_id': self.lw_id}
        res.update(self.attributes)
        return res

    @classmethod
    def from_dict(cls, data: dict):
        """ Create an instance from dict """
        try:
            lw_id = data['lw_id']
            attributes = {key: value for key, value in data.items() if key != 'lw_id'}
            return cls(lw_id, attributes)
        except (KeyError, AttributeError):
            return None  # If not all required attributes are provided, cannot create the instance        


@dataclass
class PriceValue:
    type_: PriceType
    value: float


@dataclass
class Price:
    security: Security
    source: PriceSource
    data_date: datetime.date
    modified_at: datetime.datetime
    values: List[PriceValue]

    def to_dict(self):
        """ Export an instance to dict format """
        res = {'lw_id': self.security.lw_id
            , 'data_date': self.data_date.isoformat()
            , 'source': self.source.name
            , 'modified_at': self.modified_at.isoformat()
            # , self.type_.name: self.value
        }
        values = {pv.type_.name: pv.value for pv in self.values}
        res.update(values)
        return res

    @classmethod
    def from_dict(cls, data: dict):
        """ Create an instance from dict """
        logging.debug(f'Creating price from dict: {data}')
        try:
            security = Security(data['lw_id'])
            source = PriceSource(data['source'])
            data_date = (datetime.date.fromisoformat(data['data_date']) 
                    if isinstance(data['data_date'], str) else data['data_date'])
            modified_at = (datetime.datetime.fromisoformat(data['modified_at']) 
                    if isinstance(data['modified_at'], str) else data['modified_at'])
            
            # Caller should provide a "values" dict containing types with their values, e.g. price/yield/duration
            if 'values' in data:
                values = [PriceValue(PriceType(k), v) for k, v in data['values'].items()]
            else:
                # IF not provided, fall back on any of the following fields which were provided
                values = [PriceValue(PriceType(k), data[k]) for k in data
                        if k in ('price', 'yield', 'duration')]
            logging.debug(f'Creating price with values {values}')
            return cls(security, source, data_date, modified_at, values)
        except (KeyError, AttributeError) as e:
            logging.exception(e)
            return None  # If not all required attributes are provided, cannot create the instance


@dataclass
class SecurityWithPrices:
    security: Security
    data_date: datetime.date
    prices: List[Price]

    def to_dict(self):
        """ Export an instance to dict format """
        res = self.security.to_dict()
        res['data_date'] = self.data_date.isoformat()
        res['prices'] = [px.to_dict() for px in self.prices]
        return res

    @classmethod
    def from_dict(cls, data: dict):
        """ Create an instance from dict """
        try:
            security_attributes = {k: v for k,v in data.items()
                    if k not in ('lw_id', 'data_date', 'prices')}
            security = Security(data['lw_id'], security_attributes)
            data_date = datetime.date.fromisoformat(data['data_date'])
            prices = [Price.from_dict(px_data) for px_data in data['prices']]

            # Remove any null prices
            prices = [px for px in prices if px is not None]

            if None in (security, data_date):
                return None  # If not all required attributes are provided, cannot create the instance
            return cls(security, data_date, prices)
        except (KeyError, AttributeError):
            return None  # If not all required attributes are provided, cannot create the instance


@dataclass
class PriceBatch:
    source: PriceSource
    data_date: datetime.date

    def to_dict(self):
        """ Export an instance to dict format """
        return {
            'source': self.source.name
            , 'data_date': self.data_date.isoformat()
        }

    @classmethod
    def from_dict(cls, data: dict):
        """ Create an instance from dict """
        try:
            data_date = (datetime.date.fromisoformat(data['data_date']) 
                    if isinstance(data['data_date'], str) else data['data_date'])
            return cls(PriceSource(data['source']), data_date)
        except (KeyError, AttributeError):
            return None  # If not all required attributes are provided, cannot create the instance


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



