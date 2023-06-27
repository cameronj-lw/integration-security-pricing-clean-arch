
# core python
import datetime
import logging
from typing import List, Optional, Tuple, Union

# pypi
from sqlalchemy import exc

# native
from app.domain.models import (
    Price, Security, PriceAuditEntry,
    PriceFeed, PriceFeedWithStatus, PriceSource, PriceType
)
from app.domain.repositories import (
    SecurityRepository, PriceRepository
    , PriceFeedRepository, PriceFeedWithStatusRepository
    , PriceAuditEntryRepository, PriceSourceRepository, PriceTypeRepository
)
from app.infrastructure.sql_models import (
    MGMTDBPriceFeed, MGMTDBPriceFeedWithStatus
)


from app.infrastructure.sql_tables import MonitorTable
from app.infrastructure.util.date import get_current_bday, get_previous_bday


class CoreDBSecurityRepository(SecurityRepository):
    def create(self, security: Security) -> Security:
        pass  # TODO: implement

    def get(self, lw_id: str) -> List[Security]:
        pass


class CoreDBPriceRepository(PriceRepository):
    def create(self, price: Price) -> Price:
        pass

    def get(self, data_date: Optional[datetime.date], source: Optional[PriceSource]
            , type_: Optional[PriceType], security: Optional[Security]) -> List[Price]:
        pass


class MGMTDBPriceFeedRepository(PriceFeedRepository):  # TODO: is this needed?
    def create(self, price_feed: MGMTDBPriceFeed) -> MGMTDBPriceFeed:
        pass

    def get(self, name: str) -> List[PriceFeed]:
        pass


class MGMTDBPriceFeedWithStatusRepository(PriceFeedWithStatusRepository):
    price_feed_class = MGMTDBPriceFeed

    def create(self, price_feed_with_status: PriceFeedWithStatus) -> PriceFeedWithStatus:
        pass  # TODO: implement

    def get(self, data_date: datetime.date, feeds: List[PriceFeed]) -> List[PriceFeedWithStatus]:
        result = []
        if data_date >= datetime.date(2011, 1, 1):
            data_date = get_current_bday(data_date)
        for feed in feeds:
            feed_with_status = MGMTDBPriceFeedWithStatus(feed, data_date)
            feed_with_status.update_status()
            result.append(feed_with_status)
        return result

    # def to_dict(self, feeds_with_statuses: List[MGMTDBPriceFeedWithStatus]) -> List[dict]:
        # TODO_CLEANUP: remove when not needed... 
        # result = {}
        # for fr in feeds_with_statuses:
        #     result[fr.feed.name] = {
        #         'status': fr.status,
        #         'asofdate': fr.status_ts.isoformat(),
        #         'normal_eta': fr.feed.get_normal_eta(fr.data_date).isoformat(),
        #         'security_type': fr.feed.security_type,
        #     }
        # return result


class CoreDBPriceAuditEntryRepository(PriceAuditEntryRepository):
    def create(self, price_audit_entry: PriceAuditEntry) -> PriceAuditEntry:
        pass

    def get(self, data_date: datetime.date, security: Security) -> List[PriceAuditEntry]:
        pass


class CoreDBPriceSourceRepository(PriceSourceRepository):
    def create(self, price_source: PriceSource) -> PriceSource:
        pass

    def get(self, name: str) -> List[PriceSource]:
        pass


class APXDBPriceTypeRepository(PriceTypeRepository):
    def create(self, price_type: PriceType) -> PriceType:
        pass

    def get(self, name: str) -> List[PriceType]:
        pass


