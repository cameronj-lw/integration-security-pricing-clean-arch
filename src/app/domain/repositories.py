
# core python
from abc import ABC, abstractmethod
import datetime
from typing import List, Optional, Union

# native
from app.domain.models import (
    Price, PriceBatch, Security, PriceAuditEntry, SecurityWithPrices,
    PriceFeed, PriceFeedWithStatus, PriceSource, PriceType, 
    Position, Portfolio
)


class SecurityRepository(ABC):
    @abstractmethod
    def create(self, security: Union[Security, List[Security]]) -> int:
        pass

    @abstractmethod
    def get(self, lw_id: Union[List[str],str,None] = None) -> List[Security]:
        pass


class SecuritiesForDateRepository(ABC):
    @abstractmethod
    def create(self, data_date: datetime.date, security: Union[Security, List[Security]]) -> int:
        pass

    @abstractmethod
    def get(self, data_date: datetime.date, security: Union[Security, None] = None) -> List[Security]:
        pass


class PriceRepository(ABC):
    @abstractmethod
    def create(self, prices: Union[List[Price], Price]) -> Union[List[Price], Price]:
        pass

    @abstractmethod
    def get(self, data_date: datetime.date, source: Union[PriceSource,None]=None
            , security: Union[List[Security],Security,None]=None) -> List[Price]:
        pass


class PriceBatchRepository(ABC):
    @abstractmethod
    def create(self, price_batch: PriceBatch) -> PriceBatch:
        pass

    @abstractmethod
    def get(self, data_date: Union[datetime.date,None]=None
            , source: Union[PriceSource,None]=None) -> List[PriceBatch]:
        pass


class SecurityWithPricesRepository(ABC):
    @abstractmethod
    def create(self, security_with_prices: SecurityWithPrices) -> SecurityWithPrices:
        pass

    @abstractmethod
    def add_price(self, price: Price, mode='curr') -> SecurityWithPrices:
        pass

    @abstractmethod
    def add_security(self, data_date: datetime.date, security: Security) -> SecurityWithPrices:
        pass

    @abstractmethod
    def get(self, data_date: datetime.date, security: Security) -> List[SecurityWithPrices]:
        pass


class SecuritiesWithPricesRepository(ABC):
    @abstractmethod
    def create(self, security_with_prices: SecurityWithPrices) -> SecurityWithPrices:
        pass

    @abstractmethod
    def refresh_for_securities(self, data_date: datetime.date, securities: List[Security]):
        pass

    @abstractmethod
    def get(self, data_date: datetime.date) -> List[SecurityWithPrices]:
        pass


class PositionRepository(ABC):
    @abstractmethod
    def create(self, position: Position) -> Position:
        pass

    @abstractmethod
    def get(self, data_date: datetime.date, security: Security, portfolio: Portfolio) -> List[Position]:
        pass


class PriceFeedRepository(ABC):  # TODO: is this needed?
    @abstractmethod
    def create(self, price_feed: PriceFeed) -> PriceFeed:
        pass

    @abstractmethod
    def get(self, name: str) -> List[PriceFeed]:
        pass


class PriceFeedWithStatusRepository(ABC):
    @abstractmethod
    def create(self, price_feed_with_status: PriceFeedWithStatus) -> PriceFeedWithStatus:
        pass

    @abstractmethod
    def get(self, data_date: datetime.date, feeds: List[PriceFeed]) -> List[PriceFeedWithStatus]:
        pass


class PriceAuditEntryRepository(ABC):
    @abstractmethod
    def create(self, price_audit_entry: PriceAuditEntry) -> PriceAuditEntry:
        pass

    @abstractmethod
    def get(self, data_date: datetime.date, security: Security) -> List[PriceAuditEntry]:
        pass


class PriceSourceRepository(ABC):
    @abstractmethod
    def create(self, price_source: PriceSource) -> PriceSource:
        pass

    @abstractmethod
    def get(self, name: str) -> List[PriceSource]:
        pass


class PriceTypeRepository(ABC):
    @abstractmethod
    def create(self, price_type: PriceType) -> PriceType:
        pass

    @abstractmethod
    def get(self, name: str) -> List[PriceType]:
        pass




