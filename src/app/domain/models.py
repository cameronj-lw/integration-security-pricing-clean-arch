
# core python
from dataclasses import dataclass, field
import datetime
import logging  # TODO_CLEANUP: remove when not needed ... domain layer shouldn't do logging
import math
from typing import List, Type, Union


# pypi
import numpy as np


class InvalidDictError(Exception):
    pass


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
        # TODO_CLEANUP: remove when not needed ... domain layer shouldn't do logging
        # logging.debug(f'Comparing {self.name} {HIERARCHY.index(self.name)} vs {other.name} {HIERARCHY.index(other.name)}')
        return (HIERARCHY.index(self.name) < HIERARCHY.index(other.name))


@dataclass
class PriceType:
    name: str


@dataclass 
class Security:
    lw_id: str
    attributes: dict = field(default_factory=dict)  
    # TODO: should this class require specific attributes? 
    # Or should there be multiple dicts for different types of attributes? 
    # e.g. Market IDs, classifications, LW-specific, other, ...

    def __post_init__(self):      
        # Replace np.nan and similar attributes with None.
        # This helps avoid issues when JSON (de)serializing "NaN"
        for (k,v) in self.attributes.items():
            try:
                if math.isnan(v):
                    new_dict_item = {k: None}
                    self.attributes.update(new_dict_item)
            except TypeError as e:
                continue  # e.g. "must be real number, not str"

    def to_dict(self):
        """ Export an instance to dict format """
        res = {'lw_id': self.lw_id}
        res.update(self.attributes)

        # Add 'apx_xxx' for any 'pms_xxx' keys
        apx_dict = {('apx_' + k[4:]): v for k,v in res.items() if k[:4] == 'pms_'}
        res.update(apx_dict)

        return res

    @classmethod
    def from_dict(cls, data: dict):
        """ Create an instance from dict """
        try:
            lw_id = data['lw_id']
            attributes = {key: value for key, value in data.items() if key != 'lw_id'}
            # Add 'pms_xxx' for any 'apx_xxx' keys
            pms_dict = {('pms_' + k[4:]): v for k,v in attributes.items() if k[:4] == 'apx_'}
            attributes.update(pms_dict)

            return cls(lw_id, attributes)
        except (KeyError, AttributeError):
            return None  # If not all required attributes are provided, cannot create the instance 

    def get_sec_type(self):
        if 'pms_sec_type' in self.attributes:
            return self.attributes['pms_sec_type']
        elif 'apx_sec_type' in self.attributes:
            return self.attributes['apx_sec_type']
        else:
            return None

    def is_sec_type(self, sec_type):
        """ Determine whether this Security is of provided sec type """

        # Convert to list
        if sec_type == 'bond':
            sec_types = ['cb', 'cf', 'cm', 'cv', 'fr', 'lb', 'ln', 'sf', 'tb', 'vm']
        elif sec_type == 'equity':
            sec_types = ['cc', 'ce', 'cg', 'ch', 'ci', 'cj', 'ck', 'cn', 'cr', 'cs', 'ct', 'cu', 'ps']
        else:
            sec_types = [sec_type]
        
        # Get this Security's sec type and compare
        sec_sec_type = self.get_sec_type()
        if isinstance(sec_sec_type, str):
            return self.get_sec_type()[:2] in sec_types
        else:
            return False


@dataclass
class PriceValue:
    type_: PriceType
    value: float

    # Value may be NaN... if so, replace with None here:
    # TODO_LAYERS: this creates a dependency from domain layer to numpy ... may belong somewhere else
    def __post_init__(self):
        if self.value is not None:
            # Replace np.nan and similar values with None.
            # This helps avoid issues when JSON (de)serializing "NaN"
            if math.isnan(self.value):
                self.value = None
            else:
                # The value may be of type Decimal, e.g. if originating from a sqlalchemy query.
                # This can cause issues such as when JSON serializing. Convert to standard float to avoid such issues:
                self.value = float(self.value)


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

        # Get values - separating as it's a bit involved:
        values = {pv.type_.name: pv.value for pv in self.values}

        # There may be "xyz_bid" values. If so, we also want to incldue those as "xyz" if "xyz" DNE.
        # TODO: revisit whether this is necessary?
        bid_items_dict = {k[:-4]:v for k,v in values.items() if k[-4:] == '_bid' and k[:-4] not in values}
        values.update(bid_items_dict)

        # Add values to dict and return
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
                # If not provided, fall back on any of the following fields which were provided
                values = [PriceValue(PriceType(k), data[k]) for k in data
                        if k in ('price', 'yield', 'duration')]
            logging.debug(f'Creating price with values {values}')
            return cls(security, source, data_date, modified_at, values)
        except (KeyError, AttributeError) as e:
            logging.exception(e)
            return None  # If not all required attributes are provided, cannot create the instance


@dataclass
class PriceAuditEntry:
    data_date: datetime.date
    security: Security
    reason: str
    comment: str
    before: Price
    after: Price
    modified_by: str
    modified_at: datetime.datetime

    def to_dict(self):
        return {
            "data_date": self.data_date.isoformat(),
            "lw_id": self.security.lw_id,
            "reason": self.reason,
            "comment": self.comment,
            "before": self.before.to_dict(),
            "after": self.after.to_dict(),
            "modified_by": self.modified_by,
            "modified_at": self.modified_at.isoformat(),
            "asofuser": self.modified_by,
            "asofdate": self.modified_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict):
        # TODO: validations on the data? e.g. one price per PriceType in each before & after?
        logging.info(f'Trying to create PriceAuditEntry from {data}')
        try:
            sec = Security(lw_id=data["lw_id"])
            data_date = datetime.date.fromisoformat(data["data_date"])
            modified_by=data["modified_by"] if 'modified_by' in data else data["asofuser"]
            modified_at = datetime.datetime.fromisoformat(
                    data["modified_at"] if 'modified_at' in data 
                    else datetime.datetime.fromisoformat(data["asofdate"])
            )

            # Get price values - assumption is that "before" and "after" each contain a "source", 
            # and all other numeric items in that dict represent Price Values
            before_price_values = [PriceValue(PriceType(k), v) for k,v in data['before'].items()
                    if not isinstance(v, str)] 
            after_price_values = [PriceValue(PriceType(k), v) for k,v in data['after'].items()
                    if not isinstance(v, str)] 
            
            # Create prices for before & after
            before_price = Price(security=sec, source=PriceSource(data['before']['source'])
                    , data_date=data_date, modified_at=modified_at, values=before_price_values)
            after_price = Price(security=sec, source=PriceSource(data['after']['source'])
                    , data_date=data_date, modified_at=modified_at, values=after_price_values)

            # Create & return class instance
            return cls(
                data_date=data_date,
                security=sec,
                reason=data["reason"],
                comment=data["comment"],
                before=before_price,
                after=after_price,
                modified_by=modified_by,
                modified_at=modified_at
            )
        except (KeyError, AttributeError) as e:
            logging.exception(e)
            return None  # If not all required attributes are provided, cannot create the instance


@dataclass
class SecurityWithPrices:
    """ Security with attributes, for a date, optionally accompanied by prices """

    security: Security
    data_date: datetime.date
    curr_bday_prices: Union[List[Price],None] = None
    prev_bday_price: Union[Price,None] = None
    audit_trail: Union[List[PriceAuditEntry],None] = None

    def get_chosen_price(self):
        """ Get the chosen price, based on the curr_bday_prices """
        chosen_price = None
        if self.curr_bday_prices is None:
            return None  # No prices, therefore there is no chosen one
        for px in self.curr_bday_prices:
            if chosen_price is None:
                chosen_price = px
            elif px.source > chosen_price.source:  # See PriceSource __gt__ method
                chosen_price = px
        return chosen_price

    def to_dict(self):
        """ Export an instance to dict format """
        res = self.security.to_dict()
        res['data_date'] = self.data_date.isoformat()
        res['curr_bday_prices'] = [] if self.curr_bday_prices is None else [px.to_dict() for px in self.curr_bday_prices]
        chosen_price = self.get_chosen_price()
        res['chosen_price'] = {} if chosen_price is None else chosen_price.to_dict()
        res['prev_bday_price'] = {} if self.prev_bday_price is None else self.prev_bday_price.to_dict()
        res['audit_trail'] = [] if self.audit_trail is None else [at.to_dict() for at in self.audit_trail]
        logging.debug(f'Returning to_dict result:\n{self}\n{res}')
        return res

    @classmethod
    def from_dict(cls, data: dict):
        """ Create an instance from dict """
        logging.debug(f'Creating SecurityWithPrices from dict {data}')
        try:
            security_attributes = {k: v for k,v in data.items()
                    if k not in ('lw_id', 'data_date', 'prices', 'curr_bday_prices', 'prev_bday_price', 'audit_trail', 'chosen_price')}
            security = Security(data['lw_id'], security_attributes)
            data_date = datetime.date.fromisoformat(data['data_date'])
            curr_bday_prices = None if 'curr_bday_prices' not in data else [
                Price.from_dict(px_data) for px_data in data['curr_bday_prices']]
            prev_bday_price = None
            if 'prev_bday_price' in data:
                if data['prev_bday_price'] != {}:
                    prev_bday_price = Price.from_dict(data['prev_bday_price'])
            # TODO_CLEANUP: remove once not needed
            # prev_bday_price = (None if 'prev_bday_price' not in data or isinstance(data['prev_bday_price'], list)
            #         else Price.from_dict(data['prev_bday_price']))  # TODO: why is it a list sometimes? Legacy I assume
            logging.debug(f'Looking for audit_trail in {data}')
            audit_trail = None
            if 'audit_trail' in data:
                if data['audit_trail'] is not None and data['audit_trail'] != []:
                    audit_trail = [PriceAuditEntry.from_dict(at_data) for at_data in data['audit_trail']]

            # Remove any null prices and/or audit trail
            if curr_bday_prices is not None:
                curr_bday_prices = [px for px in curr_bday_prices if px is not None]
            if audit_trail is not None:
                audit_trail = [at for at in audit_trail if at is not None]

            if None in (security, data_date):
                return None  # If not all required attributes are provided, cannot create the instance
            return cls(security, data_date, curr_bday_prices, prev_bday_price, audit_trail)
        except (KeyError, AttributeError) as e:
            # If not all required attributes are provided, cannot create the instance
            raise InvalidDictError(e)  # To be caught by callers
            return None  


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
    attributes: dict = field(default_factory=dict)  
    # TODO: should this class require specific attributes? 


@dataclass
class Position:
    portfolio: Portfolio
    data_date: datetime.date
    security: Security
    quantity: float
    is_short: bool=False
    attributes: dict=field(default_factory=dict)  
    # prices: List[Price]  # TODO: is this needed? If so, only allow one price per price type?


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



