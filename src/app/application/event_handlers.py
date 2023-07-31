
# core python
from dataclasses import dataclass
import datetime
import logging
from typing import Union

# native
from app.domain.event_handlers import EventHandler
from app.domain.event_publishers import EventPublisher
from app.domain.events import (
    SecurityCreatedEvent, PriceCreatedEvent, PriceBatchCreatedEvent,
    AppraisalBatchCreatedEvent,
    SecurityWithPricesCreatedEvent, PriceFeedCreatedEvent,
    PriceFeedWithStatusCreatedEvent, PriceAuditEntryCreatedEvent,
    PriceSourceCreatedEvent, PriceTypeCreatedEvent,
    PortfolioCreatedEvent, PositionCreatedEvent, PositionDeletedEvent
)
from app.domain.models import (
    Security, Price, SecurityWithPrices, PriceFeed,
    PriceFeedWithStatus, PriceAuditEntry, PriceSource, PriceType
)
from app.domain.repositories import (
    PriceRepository, SecurityRepository, SecurityWithPricesRepository,
    SecuritiesWithPricesRepository, PositionRepository,
    SecuritiesForDateRepository, PriceAuditEntryRepository
)
# TODO_DEPENDENCY: do dependency injection instead - app layer should not depend on infra layer
from app.infrastructure.util.config import AppConfig  
from app.infrastructure.util.date import get_next_bday, get_previous_bday
from app.infrastructure.sql_repositories import CoreDBHeldSecurityRepository



@dataclass
class SecurityCreatedEventHandler(EventHandler):
    price_repository: PriceRepository  # We'll query this to find the sec's prices
    audit_trail_repository: PriceAuditEntryRepository  # We'll query this to find the audit trail

    # We'll then update the below repos with the new Security info
    security_with_prices_repository: SecurityWithPricesRepository
    held_securities_with_prices_repository: SecuritiesWithPricesRepository

    def handle(self, event: SecurityCreatedEvent):
        """ Handle the event """
        sec = event.security
        logging.debug(f'SecurityCreatedEventHandler event: {event} type {type(event)}')
        logging.debug(f'SecurityCreatedEventHandler sec: {sec} type {type(sec)}')
        
        # Some have difficult lw_id's ... these ones are not held anyway, so do not need to be included
        if '/' in sec.lw_id:
            return True  # commit offset 

        # TODO_DEBUG: remove (timesaver)
        held_secs = CoreDBHeldSecurityRepository().get(data_date=datetime.date.today())
        if sec.lw_id not in [s.lw_id for s in held_secs]:
            return True  # commit offset  # skip if not held

        # Perform actions in response to SecurityCreatedEvent
        for d in self.get_dates_to_update():
            # Get prev and curr bday prices
            curr_bday_prices = self.price_repository.get(data_date=d, security=sec)
            prev_bday_prices = self.price_repository.get(data_date=get_previous_bday(d), security=sec, source=PriceSource('PXAPX'))
            
            # Filter to only those relevant, for curr bday
            curr_bday_prices = [px for px in curr_bday_prices if self.feed_is_relevant(px.source)]

            # Get curr bday audit trail
            curr_bday_audit_trail = self.audit_trail_repository.get(data_date=d)

            # Create SWP
            swp = SecurityWithPrices(
                security=sec, data_date=d
                , curr_bday_prices=curr_bday_prices
                , prev_bday_price=prev_bday_prices[0] if len(prev_bday_prices) else None
                , audit_trail=curr_bday_audit_trail
            )

            # Save to SecurityWithPrices repo
            self.security_with_prices_repository.create(swp)
            
            # Refresh HeldSecuritiesWithPrices repo
            self.held_securities_with_prices_repository.refresh_for_securities(data_date=d, securities=[sec])

    # TODO_REFACTOR: should this be a generic function rather than belonging to class(es)?
    def feed_is_relevant(self, source: PriceSource) -> bool:
        """ Determine whether a pricing feed is relevant.
        
        Args:
        - source (str): Name of the feed.

        Returns:
        - bool: True if the feed is relevant, else False.
        """
        relevant_price_source_names = (
            AppConfig().parser.get('app', 'vendor_price_sources').split(',')
            + AppConfig().parser.get('app', 'lw_price_sources').split(','))
        relevant_price_source_names = [s.strip() for s in relevant_price_source_names]
        # TODO: should relevant price sources be different than relevant pricing feeds in the config? 
        return (source.name in relevant_price_source_names)

    def get_dates_to_update(self, num_days_back=7):
        """ Get a list of dates which we want to update in response to a new/changed security.
        
        Args:
        - num_days_back (int): How far back to go from today, in days.

        Returns:
        - list of datetime.date: Dates for which we want to update.
        """
        return [datetime.date.today()]  # [datetime.date(year=2023, month=7, day=7)]  # TODO_DEBUG: remove this
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
    security_repository: SecurityRepository  # We'll query this to find the Security info
    audit_trail_repository: PriceAuditEntryRepository  # We'll query this to find the audit trail

    # We'll then update the following 2 repositories
    security_with_prices_repository: SecurityWithPricesRepository
    held_securities_with_prices_repository: SecuritiesWithPricesRepository

    def handle(self, event: PriceBatchCreatedEvent):
        price_batch = event.price_batch

        # Check if source is relevant. If not, return as we don't want to process it.
        # Also assign data_date here
        translated_source = self.translate_price_source(price_batch.source)
        if self.feed_is_relevant(translated_source):
            data_date = price_batch.data_date
        elif translated_source == PriceSource('APX'):
            data_date = get_next_bday(price_batch.data_date)
        else:
            return True  # commit offset  # exit - source not relevant

        # timesavers - TODO_DEBUG: remove these
        if data_date < datetime.date(year=2023, month=7, day=27):
            return True  # commit offset  # TODO_DEBUG: remove (timesaver)            
        if price_batch.source.name == 'TXPR':
            return True  # commit offset  # TODO_DEBUG: remove ... time saver

        # Get lw_id's from Price repo -> Get Securities from Security repo
        new_prices = self.price_repository.get(
            data_date=price_batch.data_date, source=price_batch.source)
        lw_ids = [px.security.lw_id for px in new_prices]
        secs = self.security_repository.get(lw_id=lw_ids)

        # Refresh for secs and return
        self.held_securities_with_prices_repository.refresh_for_securities(data_date=data_date, securities=secs)
        return True  # commit offset  # TODO_CLEANUP: remove below unreachable code? 

        logging.info(f'Processing {len(new_prices)} {price_batch.data_date} prices from {price_batch.source}')

        # Get all curr day and prev day prices, TODO: add curr bday audit trail
        curr_bday_prices = self.price_repository.get(data_date=data_date, security=secs)
        prev_bday_prices = self.price_repository.get(data_date=get_previous_bday(data_date), security=secs, source=PriceSource('PXAPX'))

        # Get curr bday audit trail
        curr_bday_audit_trail = self.audit_trail_repository.get(data_date=data_date)

        # Combine them to build SecurityWithPrices objects
        for sec in secs:
            sec_curr_bday_prices = [px for px in curr_bday_prices if px.security.lw_id == sec.lw_id]

            # Update source as translated source
            for i, px in enumerate(sec_curr_bday_prices):
                px.source = self.translate_price_source(px.source)
                sec_curr_bday_prices[i] = px

            # Filter to only those relevant
            relevant_sec_curr_bday_prices = [px for px in sec_curr_bday_prices if self.feed_is_relevant(px.source)]
            logging.debug(f'{sec.lw_id}: {len(sec_curr_bday_prices)} prices -> {len(relevant_sec_curr_bday_prices)} relevant')

            # Add prev bday APX prices
            sec_prev_bday_prices = [px for px in prev_bday_prices if px.security.lw_id == sec.lw_id]

            # Add audit trail
            sec_audit_trail = [ae for ae in curr_bday_audit_trail if ae.security.lw_id == sec.lw_id]

            # Create SWP
            swp = SecurityWithPrices(
                security=sec, data_date=data_date
                , curr_bday_prices=relevant_sec_curr_bday_prices
                , prev_bday_price=sec_prev_bday_prices[0] if len(sec_prev_bday_prices) else None
                , audit_trail=sec_audit_trail
            )
            if sec_audit_trail is not None:
                logging.info(f'Saving to SWP repo: {swp}')

            # Save to SecurityWithPrices repo
            self.security_with_prices_repository.create(swp)

        # Finally, refresh HeldSecuritiesWithPrices repo
        self.held_securities_with_prices_repository.refresh_for_securities(data_date, secs)


    def handle_old(self, event: PriceBatchCreatedEvent):
        price_batch = event.price_batch

        # Get the individual prices from the repo
        new_prices = self.price_repository.get(
            data_date=price_batch.data_date, source=price_batch.source)
        data_date = price_batch.data_date

        # TODO_DEBUG: remove below - timesaver
        # self.held_securities_with_prices_repository.refresh_for_securities(data_date=data_date, securities=[px.security for px in new_prices])
        # return

        if data_date < datetime.date(year=2023, month=7, day=10):
            return True  # commit offset  # TODO_DEBUG: remove (timesaver)
            
        if price_batch.source.name == 'TXPR':
            return True  # commit offset  # TODO_DEBUG: remove ... time saver

        translated_source = self.translate_price_source(price_batch.source)

        if price_batch.source.name == 'PXAPX':
            logging.info(f'Processing {len(new_prices)} prices from {price_batch.source}')
            data_date = get_next_bday(data_date)
            for px in new_prices:
                px.source = translated_source
                self.security_with_prices_repository.add_price(price=px, mode='prev')
        elif self.feed_is_relevant(translated_source):
            # return  # TODO_DEBUG: remove (timesaver)
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
            return True  # commit offset
        # now, refresh master read model:
        self.held_securities_with_prices_repository.refresh_for_securities(data_date=data_date, securities=[px.security for px in new_prices])
        return True  # commit offset

    # TODO_REFACTOR: should this be a generic function rather than belonging to class(es)?
    def feed_is_relevant(self, source: PriceSource) -> bool:
        """ Determine whether a pricing feed is relevant.
        
        Args:
        - source (str): Name of the feed.

        Returns:
        - bool: True if the feed is relevant, else False.
        """
        relevant_price_source_names = (
            AppConfig().parser.get('app', 'vendor_price_sources').split(',')
            + AppConfig().parser.get('app', 'lw_price_sources').split(','))
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
        elif source.name == 'PXAPX':
            return PriceSource('APX')
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

        # TODO_DEBUG: remove (timesaver)
        if appraisal_batch.data_date < datetime.date(year=2023, month=7, day=26):
            return True  # commit offset

        # Perform actions in response to AppraisalBatchCreatedEvent
        # TODO: inspect "portfolios" here, and ignore if not @LW_ALL_OpenAndMeasurement?
        # positions = self.position_repository.get(data_date=appraisal_batch.data_date)
        # next_bday = get_next_bday(appraisal_batch.data_date)
        # held_secs = [pos.security for pos in positions]
        # held_secs = set(held_secs)  # list(dict.fromkeys(held_secs))  # to remove dupes

        # Get list of held secs
        held_secs = self.position_repository.get_unique_securities(data_date=appraisal_batch.data_date)

        # Update master RM, removing securities which are not in the Appraisal result because they are not held
        logging.info(f'Refreshing HSWP repo for {appraisal_batch.data_date}')
        self.held_securities_with_prices_repository.refresh_for_securities(data_date=appraisal_batch.data_date, securities=held_secs, remove_other_secs=True)
        logging.info(f'Done refreshing HSWP repo for {appraisal_batch.data_date}')
        
        # Also refresh for next bday
        next_bday = get_next_bday(appraisal_batch.data_date)
        logging.info(f'Refreshing HSWP repo for {next_bday}')
        self.held_securities_with_prices_repository.refresh_for_securities(data_date=next_bday, securities=held_secs, remove_other_secs=True)
        logging.info(f'Done refreshing HSWP repo for {next_bday}')
        return True  # commit offset

    # TODO: remove below when not needed, i.e. once confirmed retiring the held_securities read model
    def handle_old(self, event: AppraisalBatchCreatedEvent):
        """ Handle the event """
        appraisal_batch = event.appraisal_batch

        # TODO_DEBUG: remove (timesaver)
        if appraisal_batch.data_date != datetime.date(year=2023, month=7, day=17):
            return

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

        # Update master RM, removing securities which are not in the Appraisal result because they are not held
        # self.held_securities_with_prices_repository.refresh_for_securities(data_date=appraisal_batch.data_date, securities=held_secs, remove_other_secs=True)


@dataclass
class PositionEventHandler(EventHandler):
    position_event_publisher: EventPublisher

    def handle(self, event: Union[PositionCreatedEvent, PositionDeletedEvent]):
        """ Handle the event """
        # Publish using the provided event publisher
        try:
            self.position_event_publisher.publish(event)
        except Exception as ex:
            logging.exception(ex)
        return True  # commit offset

    def handle_deserialization_error(self, ex):
        """ What to do when deserialization fails """
        logging.exception(ex)
        return True  # commit offset


@dataclass
class PortfolioCreatedEventHandler(EventHandler):
    portfolio_event_publisher: EventPublisher

    def handle(self, event: PortfolioCreatedEvent):
        """ Handle the event """
        # Publish using the provided event publisher
        try:
            self.portfolio_event_publisher.publish(event)
        except Exception as ex:
            logging.exception(ex)
        return True  # commit offset

    def handle_deserialization_error(self, ex):
        """ What to do when deserialization fails """
        logging.exception(ex)
        return True  # commit offset


