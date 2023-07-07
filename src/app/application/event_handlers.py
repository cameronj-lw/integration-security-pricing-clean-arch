
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
# TODO_DEPENDENCY: do dependency injection instead - app layer should not depend on infra layer
from app.infrastructure.util.config import AppConfig  
from app.infrastructure.util.date import get_next_bday



@dataclass
class SecurityCreatedEventHandler(EventHandler):
    # We'll update the below repos with the new Security info
    security_with_prices_repository: SecurityWithPricesRepository
    held_securities_with_prices_repository: SecuritiesWithPricesRepository

    def handle(self, event: SecurityCreatedEvent):
        """ Handle the event """
        security = event.security
        
        # Some have difficult lw_id's ... these ones are not held anyway, so do not need to be included
        if '/' in security.lw_id:
            return

        # Perform actions in response to SecurityCreatedEvent
        for d in self.get_dates_to_update():
            self.security_with_prices_repository.add_security(data_date=d, security=security)
            self.held_securities_with_prices_repository.refresh_for_securities(data_date=d, securities=[security])

    def get_dates_to_update(self, num_days_back=14):
        """ Get a list of dates which we want to update in response to a new/changed security.
        
        Args:
        - num_days_back (int): How far back to go from today, in days.

        Returns:
        - list of datetime.date: Dates for which we want to update.
        """
        res = []
        # Start with today
        today = date = datetime.date.today()
        while date > today - datetime.timedelta(days=num_days_back):
            res.append(date)
            date -= datetime.timedelta(days=1)
        return res


# TODO_CLEANUP: remove when not needed
# class PriceCreatedEventHandler(EventHandler):
#     def handle(self, event: PriceCreatedEvent):
#         """ Handle the event """
#         price = event.price
#         # Perform actions in response to PriceCreatedEvent
#         print(f"Handling PriceCreatedEvent: {price}")




@dataclass
class PriceBatchCreatedEventHandler(EventHandler):
    price_repository: PriceRepository  # We'll query this to find the new prices
    # We'll then update the following 2 repositories
    security_with_prices_repository: SecurityWithPricesRepository
    held_securities_with_prices_repository: SecuritiesWithPricesRepository

    def handle(self, event: PriceBatchCreatedEvent):
        price_batch = event.price_batch
        # Perform actions in response to PriceBatchCreatedEvent
        # print(f"Handling PriceBatchCreatedEvent: {price_batch}")
        new_prices = self.price_repository.get_prices(
            data_date=price_batch.data_date, source=price_batch.source)
        data_date = price_batch.data_date
        source = self.translate_price_source(price_batch.source)
        if price_batch.source == 'PXAPX':
            data_date = get_next_bday(data_date)
            for px in new_prices:
                self.security_with_prices_repository.add_price(price=px, mode='prev')
        elif self.feed_is_relevant(source):
            for px in new_prices:
                self.security_with_prices_repository.add_price(price=px)
        else:
            return  # TODO: should commit consumer offset here?
        # now, refresh master read model:
        self.held_securities_with_prices_repository.refresh_for_securities(data_date=data_date, securities=[px.security for px in new_prices])
        # TODO: should commit consumer offset here?

    def feed_is_relevant(self, source: str) -> bool:
        """ Determine whether a pricing feed is relevant.
        
        Args:
        - source (str): Name of the feed.

        Returns:
        - bool: True if the feed is relevant, else False.
        """
        relevant_price_sources = AppConfig().parser.get('app', 'relevant_pricing_feeds').split(',')
        relevant_price_sources = [s.trim() for s in relevant_price_sources]
        # TODO: should relevant price sources be different than relevant pricing feeds in the config? 
        return (source in relevant_price_sources)

    def translate_price_source(self, source: str) -> str:
        """ Translate the price source from the batch into a more generic source
        
        Args:
        - source (str): Name of the source.

        Returns:
        - str: Translated source.
        """
        if source[:3] == 'BB_' and '_DERIVED' not in source:
            return 'BLOOMBERG'
        elif x == 'FTSETMX_PX':
            return 'FTSE'
        elif x == 'MARKIT_LOAN_CLEANPRICE':
            return 'MARKIT'
        elif x == 'FUNDRUN_EQUITY':
            return 'FUNDRUN'
        elif x in ('FIDESK_MANUALPRICE', 'LW_OVERRIDE'):
            return 'OVERRIDE'
        elif x in ('FIDESK_MISSINGPRICE', 'LW_MANUAL'):
            return 'MANUAL'
        else:
            return source


@dataclass
class AppraisalBatchCreatedEventHandler(EventHandler):
    # The appraisal results will be retrieved from here
    position_repository: PositionRepository

    # We will then update the following repos with the new list of held securities
    held_securities_repository: SecurityRepository
    held_securities_with_prices_repository: SecuritiesWithPricesRepository

    def handle(self, event: AppraisalBatchCreatedEvent):
        """ Handle the event """
        appraisal_batch = event.appraisal_batch
        # Perform actions in response to AppraisalBatchCreatedEvent
        # TODO: inspect "portfolios" here, and ignore if not @LW_ALL_OpenAndMeasurement?
        # positions = self.position_repository.get(data_date=appraisal_batch.data_date)
        # next_bday = get_next_bday(appraisal_batch.data_date)
        # held_secs = [pos.security for pos in positions]
        # held_secs = set(held_secs)  # list(dict.fromkeys(held_secs))  # to remove dupes

        # Get unique securities
        held_secs = self.position_repository.get_unique_securities(data_date=appraisal_batch.data_date)

        # update held securities RM for appraisal date:
        self.held_securities_repository.create(data_date=appraisal_batch.data_date, securities=held_secs)
        # update master RM:
        self.held_securities_with_prices_repository.refresh_for_securities(data_date=get_next_bday(appraisal_batch.data_date), securities=held_secs)


