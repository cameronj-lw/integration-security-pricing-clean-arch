
# core python
from configparser import ConfigParser
from dataclasses import dataclass
import datetime
from typing import List, Optional

# native
from app.domain.models import PriceFeed, PriceFeedWithStatus, Security
from app.domain.repositories import (
    PriceFeedWithStatusRepository, SecurityRepository
)
from app.infrastructure.util.config import AppConfig


@dataclass
class PriceFeedWithStatusQueryHandler:
    repo: PriceFeedWithStatusRepository

    def get_relevant_pricing_feeds(self):
        unparsed = AppConfig().get("app", "relevant_pricing_feeds")
        parsed = [feed.strip() for feed in unparsed.split(',')]
        return [self.repo.price_feed_class(f) for f in parsed]

    def handle(self, data_date: datetime.date, feed: Optional[PriceFeed]=None) -> List[PriceFeedWithStatus]:
        # If feed is not provided, defer to config for relevant feeds:
        if feed is None:
            feeds = self.get_relevant_pricing_feeds()
        else:
            feeds = [feed]
        # Retrieve the price feeds with statuses from the repository
        return self.repo.get(data_date, feeds)


class PriceAuditReasonQueryHandler:

    def handle(self) -> List[str]:
        return [
            {'reason': '#1 - Index does not price this bond'}
            , {'reason': '#2 - Index provided the bond price late by email'}
            , {'reason': '#3 - Index corrected a wrong bond price late by email'}
            , {'reason': '#4 - Index price appears to be in error. LW substituted price with consensus live broker quote'}
            , {'reason': '#5 - Forced to manually price bond to control duration inputs for risk management'}
            , {'reason': '#6 - New Issue'}
            , {'reason': '#7 - Other'}
        ]


@dataclass
class ManualPricingSecurityQueryHandler:
    repo: SecurityRepository

    def handle(self) -> List[Security]:
        return self.repo.get()

