
# core python
from dataclasses import dataclass

# native
from app.domain.event_publishers import DomainEventPublisher
from app.domain.events import (
    SecurityCreatedEvent, PriceCreatedEvent,
    SecurityWithPricesCreatedEvent, PriceFeedCreatedEvent,
    PriceFeedWithStatusCreatedEvent, PriceAuditEntryCreatedEvent,
    PriceSourceCreatedEvent, PriceTypeCreatedEvent
)
from app.domain.models import (
    Security, Price, SecurityWithPrices, PriceFeed,
    PriceFeedWithStatus, PriceAuditEntry, PriceSource, PriceType
)
from app.domain.repositories import (
    SecurityRepository, PriceRepository, SecurityWithPricesRepository, PriceFeedRepository,
    PriceFeedWithStatusRepository, PriceAuditEntryRepository, PriceSourceRepository, PriceTypeRepository
)


@dataclass
class SecurityService:
    security_repository: SecurityRepository
    event_publisher: DomainEventPublisher

    def create_security(self, security: Security):
        # Perform security-specific logic and validation

        # Save the security to the repository
        created_security = self.security_repository.create(security)

        # Publish the SecurityCreatedEvent
        event = SecurityCreatedEvent(security=created_security)
        self.event_publisher.publish(event)


@dataclass
class PriceService:
    price_repository: PriceRepository
    event_publisher: DomainEventPublisher

    def create_price(self, price: Price):
        # Perform price-specific logic and validation

        # Save the price to the repository
        created_price = self.price_repository.create(price)

        # Publish the PriceCreatedEvent
        event = PriceCreatedEvent(price=created_price)
        self.event_publisher.publish(event)


@dataclass
class SecurityWithPricesService:
    security_with_prices_repository: SecurityWithPricesRepository
    event_publisher: DomainEventPublisher

    def create_security_with_prices(self, security_with_prices: SecurityWithPrices):
        # Perform security with prices-specific logic and validation

        # Save the security with prices to the repository
        created_security_with_prices = self.security_with_prices_repository.create(security_with_prices)

        # Publish the SecurityWithPricesCreatedEvent
        event = SecurityWithPricesCreatedEvent(security_with_prices=created_security_with_prices)
        self.event_publisher.publish(event)


@dataclass
class PriceFeedService:
    price_feed_repository: PriceFeedRepository
    event_publisher: DomainEventPublisher

    def create_price_feed(self, price_feed: PriceFeed):
        # Perform price feed-specific logic and validation

        # Save the price feed to the repository
        created_price_feed = self.price_feed_repository.create(price_feed)

        # Publish the PriceFeedCreatedEvent
        event = PriceFeedCreatedEvent(price_feed=created_price_feed)
        self.event_publisher.publish(event)


@dataclass
class PriceFeedWithStatusService:
    price_feed_with_status_repository: PriceFeedWithStatusRepository
    event_publisher: DomainEventPublisher

    def create_price_feed_with_status(self, price_feed_with_status: PriceFeedWithStatus):
        # Perform price feed with status-specific logic and validation

        # Save the price feed with status to the repository
        created_price_feed_with_status = self.price_feed_with_status_repository.create(price_feed_with_status)

        # Publish the PriceFeedWithStatusCreatedEvent
        event = PriceFeedWithStatusCreatedEvent(price_feed_with_status=created_price_feed_with_status)
        self.event_publisher.publish(event)


@dataclass
class PriceAuditEntryService:
    price_audit_entry_repository: PriceAuditEntryRepository
    event_publisher: DomainEventPublisher

    def create_price_audit_entry(self, price_audit_entry: PriceAuditEntry):
        # Perform price audit entry-specific logic and validation

        # Save the price audit entry to the repository
        created_price_audit_entry = self.price_audit_entry_repository.create(price_audit_entry)

        # Publish the PriceAuditEntryCreatedEvent
        event = PriceAuditEntryCreatedEvent(price_audit_entry=created_price_audit_entry)
        self.event_publisher.publish(event)


@dataclass
class PriceSourceService:
    price_source_repository: PriceSourceRepository
    event_publisher: DomainEventPublisher

    def create_price_source(self, price_source: PriceSource):
        # Perform price source-specific logic and validation

        # Save the price source to the repository
        created_price_source = self.price_source_repository.create(price_source)

        # Publish the PriceSourceCreatedEvent
        event = PriceSourceCreatedEvent(price_source=created_price_source)
        self.event_publisher.publish(event)


@dataclass
class PriceTypeService:
    price_type_repository: PriceTypeRepository
    event_publisher: DomainEventPublisher

    def create_price_type(self, price_type: PriceType):
        # Perform price type-specific logic and validation

        # Save the price type to the repository
        created_price_type = self.price_type_repository.create(price_type)

        # Publish the PriceTypeCreatedEvent
        event = PriceTypeCreatedEvent(price_type=created_price_type)
        self.event_publisher.publish(event)
