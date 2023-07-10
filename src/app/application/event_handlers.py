
# core python
from dataclasses import dataclass
import datetime
import logging

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
    SecuritiesWithPricesRepository, PositionRepository,
    SecuritiesForDateRepository
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
        return [datetime.date(year=2023, month=7, day=7)]  # for debug
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

        # Get the individual prices from the repo
        new_prices = self.price_repository.get(
            data_date=price_batch.data_date, source=price_batch.source)
        data_date = price_batch.data_date

        # TODO_DEBUG: remove below - timesaver
        self.held_securities_with_prices_repository.refresh_for_securities(data_date=data_date, securities=[px.security for px in new_prices])
        return

        if data_date != datetime.date(year=2023, month=7, day=7):
            return  # TODO_DEBUG: remove?
            
        translated_source = self.translate_price_source(price_batch.source)

        if price_batch.source == 'TXPR':
            return  # TODO_DEBUG: remove ... time saver

        if price_batch.source == 'PXAPX':
            data_date = get_next_bday(data_date)
            for px in new_prices:
                px.source = translated_source
                self.security_with_prices_repository.add_price(price=px, mode='prev')
        elif self.feed_is_relevant(translated_source):
            logging.info(f'Processing {len(new_prices)} prices from {price_batch.source}')
            logged = False
            for px in new_prices:
                # if px.security.lw_id != 'COCC0000498':
                #     continue
                px.source = translated_source
                if not logged:
                    logging.info(f'Adding price {px}')
                    logged = True
                
                # Update 
                self.security_with_prices_repository.add_price(price=px)
        else:
            # If we reached here, the feed is not relevant and we won't bother processing it
            return  # TODO: should commit consumer offset here?
        # now, refresh master read model:
        self.held_securities_with_prices_repository.refresh_for_securities(data_date=data_date, securities=[px.security for px in new_prices])
        # TODO: should commit consumer offset here?

    def feed_is_relevant(self, source: PriceSource) -> bool:
        """ Determine whether a pricing feed is relevant.
        
        Args:
        - source (str): Name of the feed.

        Returns:
        - bool: True if the feed is relevant, else False.
        """
        relevant_price_source_names = AppConfig().parser.get('app', 'relevant_pricing_feeds').split(',')
        relevant_price_source_names = [s.strip() for s in relevant_price_source_names]
        # TODO: should relevant price sources be different than relevant pricing feeds in the config? 
        return (source.name in relevant_price_source_names)

    def translate_price_source(self, source: PriceSource) -> PriceSource:
        """ Translate the price source from the batch into a more generic source
        
        Args:
        - source (str): Name of the source.

        Returns:
        - str: Translated source.
        """
        if source.name[:3] == 'BB_' and '_DERIVED' not in source.name:
            return PriceSource('BLOOMBERG')
        elif source.name == 'FTSETMX_PX':
            return PriceSource('FTSE')
        elif source.name == 'MARKIT_LOAN_CLEANPRICE':
            return PriceSource('MARKIT')
        elif source.name == 'FUNDRUN_EQUITY':
            return PriceSource('FUNDRUN')
        elif source.name in ('FIDESK_MANUALPRICE', 'LW_OVERRIDE'):
            return PriceSource('OVERRIDE')
        elif source.name in ('FIDESK_MISSINGPRICE', 'LW_MANUAL'):
            return PriceSource('MANUAL')
        else:
            return source


@dataclass
class AppraisalBatchCreatedEventHandler(EventHandler):
    # The appraisal results will be retrieved from here
    position_repository: PositionRepository

    # We will then update the following repos with the new list of held securities
    held_securities_repository: SecuritiesForDateRepository
    held_securities_with_prices_repository: SecuritiesWithPricesRepository

    def handle(self, event: AppraisalBatchCreatedEvent):
        """ Handle the event """
        appraisal_batch = event.appraisal_batch
        next_bday = get_next_bday(appraisal_batch.data_date)
        # Perform actions in response to AppraisalBatchCreatedEvent
        # TODO: inspect "portfolios" here, and ignore if not @LW_ALL_OpenAndMeasurement?
        # positions = self.position_repository.get(data_date=appraisal_batch.data_date)
        # next_bday = get_next_bday(appraisal_batch.data_date)
        # held_secs = [pos.security for pos in positions]
        # held_secs = set(held_secs)  # list(dict.fromkeys(held_secs))  # to remove dupes

        # Get unique securities
        held_secs = self.position_repository.get_unique_securities(data_date=appraisal_batch.data_date)

        # Update held securities RM for appraisal date
        self.held_securities_repository.create(data_date=appraisal_batch.data_date, securities=held_secs)

        # Update held securities RM for next bday, if the file DNE
        if self.held_securities_repository.get(data_date=next_bday) is None:
            self.held_securities_repository.create(data_date=next_bday, securities=held_secs)

        # Update master RM
        self.held_securities_with_prices_repository.refresh_for_securities(data_date=appraisal_batch.data_date, securities=held_secs)


