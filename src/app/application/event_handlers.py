
# core python
from dataclasses import dataclass
import datetime

# native
from app.domain.event_handlers import EventHandler
from app.domain.events import (
    SecurityCreatedEvent, PriceCreatedEvent, PriceBatchCreatedEvent,
    AppraisalBatchCreatedEvent,
    SecurityWithPricesCreatedEvent, PriceFeedCreatedEvent,
    PriceFeedWithStatusCreatedEvent, PriceAuditEntryCreatedEvent,
    PriceSourceCreatedEvent, PriceTypeCreatedEvent
)
from app.domain.models import (
    Security, Price, SecurityWithPrices, PriceFeed,
    PriceFeedWithStatus, PriceAuditEntry, PriceSource, PriceType
)
from app.domain.repositories import (
    PriceRepository, SecurityRepository, SecurityWithPricesRepository,
    SecuritiesWithPricesRepository, PositionRepository
)


def get_dates_to_update():  # TODO: implement properly; move to better spot?
    return [datetime.date(2023, 6, 20)]


@dataclass
class SecurityCreatedEventHandler(EventHandler):
    # We'll update the below repos with the new Security info
    security_with_prices_repository: SecurityWithPricesRepository
    securities_with_prices_repository: SecuritiesWithPricesRepository

    def handle(self, event: SecurityCreatedEvent):
        security = event.security
        # Perform actions in response to SecurityCreatedEvent
        print(f"Handling SecurityCreatedEvent: {security}")        
        for d in get_dates_to_update():
            self.security_with_prices_repository.add_security(data_date=d, security=security)
            self.securities_with_prices_repository.refresh(data_date=d, security=security)


class PriceCreatedEventHandler(EventHandler):
    def handle(self, event: PriceCreatedEvent):
        price = event.price
        # Perform actions in response to PriceCreatedEvent
        print(f"Handling PriceCreatedEvent: {price}")

def translate_price_source(source: str) -> str:
    return source  # TODO: placeholder - implement + put in desired place

def is_relevant(source: str) -> bool:
    return True  # TODO: placeholder - implement + put in desired place

def get_next_bday(data_date: datetime.date) -> datetime.date:
    return data_date  # TODO: placeholder - implement + put in desired place

@dataclass
class PriceBatchCreatedEventHandler(EventHandler):
    price_repository: PriceRepository  # We'll query this to find the new prices
    security_with_prices_repository: SecurityWithPricesRepository
    securities_with_prices_repository: SecuritiesWithPricesRepository

    def handle(self, event: PriceBatchCreatedEvent):
        price_batch = event.price_batch
        # Perform actions in response to PriceBatchCreatedEvent
        # print(f"Handling PriceBatchCreatedEvent: {price_batch}")
        new_prices = self.price_repository.get_prices(
            data_date=price_batch.data_date, source=price_batch.source)
        data_date = price_batch.data_date
        source = translate_price_source(price_batch.source)
        if price_batch.source == 'PXAPX':
            data_date = get_next_bday(data_date)
            for px in new_prices:
                self.security_with_prices_repository.add_price(price=px, mode='prev')
        elif is_relevant(source):
            for px in new_prices:
                self.security_with_prices_repository.add_price(price=px)
        else:
            return  # TODO: should commit consumer offset here?
        # now, refresh master read model:
        self.securities_with_prices_repository.refresh(data_date=data_date, securities=[px.security for px in new_prices])
        # TODO: should commit consumer offset here?


@dataclass
class AppraisalBatchCreatedEventHandler(EventHandler):
    position_repository: PositionRepository
    held_security_repository: SecurityRepository
    held_securities_with_prices_repository: SecuritiesWithPricesRepository

    def handle(self, event: AppraisalBatchCreatedEvent):
        appraisal_batch = event.appraisal_batch
        # Perform actions in response to AppraisalBatchCreatedEvent
        print(f"Handling AppraisalBatchCreatedEvent: {appraisal_batch}")
        # TODO: inspect "portfolios" here, and ignore if not @LW_ALL_OpenAndMeasurement?
        positions = self.position_repository.get(data_date=appraisal_batch.data_date)
        next_bday = get_next_bday(appraisal_batch.data_date)
        held_secs = [pos.security for pos in positions]
        held_secs = list(dict.fromkeys(held_secs))  # to remove dupes
        # update held securities RM:
        self.held_security_repository.create(security=held_secs)
        # update master RM:
        self.held_securities_with_prices_repository.refresh(data_date=appraisal_batch.data_date, securities=held_secs)


class SecurityWithPricesCreatedEventHandler(EventHandler):
    def handle(self, event: SecurityWithPricesCreatedEvent):
        security_with_prices = event.security_with_prices
        # Perform actions in response to SecurityWithPricesCreatedEvent
        print(f"Handling SecurityWithPricesCreatedEvent: {security_with_prices}")


class PriceFeedCreatedEventHandler(EventHandler):
    def handle(self, event: PriceFeedCreatedEvent):
        price_feed = event.price_feed
        # Perform actions in response to PriceFeedCreatedEvent
        print(f"Handling PriceFeedCreatedEvent: {price_feed}")


class PriceFeedWithStatusCreatedEventHandler(EventHandler):
    def handle(self, event: PriceFeedWithStatusCreatedEvent):
        price_feed_with_status = event.price_feed_with_status
        # Perform actions in response to PriceFeedWithStatusCreatedEvent
        print(f"Handling PriceFeedWithStatusCreatedEvent: {price_feed_with_status}")


class PriceAuditEntryCreatedEventHandler(EventHandler):
    def handle(self, event: PriceAuditEntryCreatedEvent):
        price_audit_entry = event.price_audit_entry
        # Perform actions in response to PriceAuditEntryCreatedEvent
        print(f"Handling PriceAuditEntryCreatedEvent: {price_audit_entry}")


class PriceSourceCreatedEventHandler(EventHandler):
    def handle(self, event: PriceSourceCreatedEvent):
        price_source = event.price_source
        # Perform actions in response to PriceSourceCreatedEvent
        print(f"Handling PriceSourceCreatedEvent: {price_source}")


class PriceTypeCreatedEventHandler(EventHandler):
    def handle(self, event: PriceTypeCreatedEvent):
        price_type = event.price_type
        # Perform actions in response to PriceTypeCreatedEvent
        print(f"Handling PriceTypeCreatedEvent: {price_type}")
