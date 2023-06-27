
# core python
import datetime
from typing import List

# native
from app.domain.models import (
    Price, Security, PriceAuditEntry, SecurityWithPrices
)
from app.domain.repositories import (
    SecurityRepository, SecurityWithPricesRepository
    , SecuritiesWithPricesRepository
    , PriceAuditEntryRepository
)


class JSONReadModelHeldSecurityRepository(SecurityRepository):
    def create(self, security: Security) -> Security:
        pass  # TODO: implement

    def get(self, lw_id: str) -> List[Security]:
        pass


class JSONReadModelHeldSecuritiesWithPricesRepository(SecuritiesWithPricesRepository):
    def create(self, security_with_prices: SecurityWithPrices) -> SecurityWithPrices:
        pass

    def get(self, data_date: datetime.date) -> List[SecurityWithPrices]:
        pass


class JSONReadModelSecurityWithPricesRepository(SecurityWithPricesRepository):
    def create(self, security_with_prices: SecurityWithPrices) -> SecurityWithPrices:
        pass

    def add_price(self, price: Price, mode='curr') -> SecurityWithPrices:
        pass

    def get(self, data_date: datetime.date, security: Security) -> List[SecurityWithPrices]:
        pass


class JSONReadModelPriceAuditEntryRepository(PriceAuditEntryRepository):
    def create(self, price_audit_entry: PriceAuditEntry) -> PriceAuditEntry:
        pass

    def get(self, data_date: datetime.date, security: Security) -> List[PriceAuditEntry]:
        pass


