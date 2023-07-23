
# core python
from configparser import ConfigParser
from dataclasses import dataclass
import datetime
import logging
from typing import List, Optional

# native
from app.application.models import UserWithColumnConfig, DateWithPricingAttachments
from app.application.repositories import UserWithColumnConfigRepository, DateWithPricingAttachmentsRepository
from app.domain.models import PriceFeed, PriceFeedWithStatus, Security, PriceAuditEntry
from app.domain.repositories import (
    PriceFeedWithStatusRepository, SecurityRepository, SecuritiesWithPricesRepository
    , PriceAuditEntryRepository
)
from app.infrastructure.util.config import AppConfig


@dataclass
class PriceFeedWithStatusQueryHandler:
    repo: PriceFeedWithStatusRepository

    def get_relevant_pricing_feeds(self):
        """ Get list of pricing feeds which are relevant """
        unparsed = AppConfig().get("app", "vendor_price_sources")
        parsed = [feed.strip() for feed in unparsed.split(',')]
        return [self.repo.price_feed_class(f) for f in parsed]

    def handle(self, data_date: datetime.date, feed: Optional[PriceFeed]=None) -> List[PriceFeedWithStatus]:
        """ Handle the query """
        # If feed is not provided, defer to config for relevant feeds:
        if feed is None:
            feeds = self.get_relevant_pricing_feeds()
        else:
            feeds = [feed]
            
        # Retrieve the price feeds with statuses from the repository
        return self.repo.get(data_date, feeds)


class PriceAuditReasonQueryHandler:

    def handle(self) -> List[str]:
        """ Handle the query """
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
class PriceAuditEntryQueryHandler:
    repo: PriceAuditEntryRepository

    def handle(self, data_date: str) -> List[PriceAuditEntry]:
        """ Handle the query """
        try:
            date = datetime.datetime.strptime(data_date, '%Y%m%d').date()
        except Exception as e:
            # TODO: application error handling for invalid date?
            pass  # exception should be caught by interface layer
        return self.repo.get(date)


@dataclass
class ManualPricingSecurityQueryHandler:
    repo: SecurityRepository

    def handle(self) -> List[Security]:
        """ Handle the query """
        return self.repo.get()


@dataclass
class UserWithColumnConfigQueryHandler:
    repo: UserWithColumnConfigRepository

    def handle(self, user_id: str) -> UserWithColumnConfig:
        """ Handle the query """
        return self.repo.get(user_id)


@dataclass
class PricingAttachmentByDateQueryHandler:
    repo: DateWithPricingAttachmentsRepository

    def handle(self, data_date: str) -> DateWithPricingAttachments:
        """ Handle the query """
        try:
            date = datetime.datetime.strptime(data_date, '%Y%m%d').date()
        except Exception as e:
            # TODO: application error handling for invalid date?
            pass  # exception should be caught by interface layer
        return self.repo.get(date)


@dataclass
class PriceCountBySourceQueryHandler:
    repo: SecuritiesWithPricesRepository

    def handle(self, payload) -> dict:  # TODO: should this return a domain object rather than dict?
        """ Handle the query """
        # Populate defaults
        try:
            data_date = datetime.date.today() if 'price_date' not in payload else (
                    datetime.datetime.strptime(payload['price_date'], '%Y%m%d').date())
        except Exception as e:
            # TODO: application error handling for invalid date?
            pass  # exception should be caught by interface layer
        
        sec_type = None if 'sec_type' not in payload else payload['sec_type']
            
        # Retrieve the held securities with prices from the repository
        held_secs_with_prices = self.repo.get(data_date=data_date)

        # Remove any which are null
        held_secs_with_prices = [swp for swp in held_secs_with_prices if swp is not None]

        # Now start building the dict  # TODO: does this logic belong in infra layer? 
        res = {}
        for swp in held_secs_with_prices:
            if sec_type is not None:
                try:
                    logging.debug(f'Checking whether {swp} is sec type {sec_type}')
                    if not swp.security.is_sec_type(sec_type):
                        continue  # Skip, as it is not of requested sec type
                except TypeError as e:
                    logging.error(e)
                    logging.error(swp)
            chosen_price = swp.get_chosen_price()
            if chosen_price is None:
                swp_source = 'MISSING'
            else:
                swp_source = chosen_price.source.name
            if swp_source not in res:
                # Add if DNE, with count of 1
                res[swp_source] = 1
            else:
                # Otherwise, increment:
                res[swp_source] += 1
        return res


@dataclass
class HeldSecurityPriceQueryHandler:
    repo: SecuritiesWithPricesRepository

    def handle(self, payload) -> dict:  # TODO: should this return a domain object rather than dict?
        """ Handle the query """
        logging.info(f'HeldSecurityPrice payload: {payload}')
        # Populate defaults
        try:
            data_date = datetime.date.today() if 'price_date' not in payload else (
                    datetime.datetime.strptime(payload['price_date'], '%Y%m%d').date())
        except Exception as e:
            # TODO: application error handling for invalid date?
            pass  # exception should be caught by interface layer
        
        sec_type = None if 'sec_type' not in payload else payload['sec_type']
        source_name = None if 'source' not in payload else payload['source'].upper()
            
        # Retrieve the held securities with prices from the repository
        held_secs_with_prices = self.repo.get(data_date=data_date)

        # Now filter by source and/or sec type:
        res = []
        for swp in held_secs_with_prices:
            if sec_type is not None:
                if not swp.security.is_sec_type(sec_type):
                    continue  # Skip, as it is not of requested sec type
            if source_name is not None:
                chosen_price = swp.get_chosen_price()
                if chosen_price is None:
                    # No chosen price -> should include only in 'MISSING':
                    if source_name == 'MISSING':
                        res.append(swp.to_dict())
                    continue
                elif chosen_price.source.name != source_name:
                    continue
            # If we reached here, the SWP should be included. Add it to results:
            res.append(swp.to_dict())
        
        logging.info(f'HeldSecurityPrice returning {len(res)} rows')
        return res

