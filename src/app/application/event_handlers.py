
# core python
from dataclasses import dataclass
import datetime
import logging
from typing import Union

# native
from app.domain.event_handlers import EventHandler
from app.domain.event_publishers import EventPublisher
from app.domain.events import (
    SecurityCreatedEvent, PriceBatchCreatedEvent,
    AppraisalBatchCreatedEvent,
    PortfolioCreatedEvent, PositionCreatedEvent, PositionDeletedEvent
)
from app.domain.models import (
    Security, Price, SecurityWithPrices, PriceFeed,
    PriceAuditEntry, PriceSource
)
from app.domain.repositories import (
    PriceRepository, SecurityRepository, SecurityWithPricesRepository,
    SecuritiesWithPricesRepository, PositionRepository, PortfolioRepository,
    PriceAuditEntryRepository
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
        # return [datetime.date.today()]  # [datetime.date(year=2023, month=7, day=7)]  # TODO_DEBUG: remove this
        res = []
        # Start with today
        today = date = datetime.date.today()
        while date > today - datetime.timedelta(days=num_days_back):
            res.append(date)
            date -= datetime.timedelta(days=1)
        return res


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

    # We will then update the following repo with the new held securities
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


@dataclass
class PositionEventHandler(EventHandler):
    # We will upsert to the following repo
    position_repo: PositionRepository
    # We will use the following repo to retrieve the lw_id, if needed
    security_repo: SecurityRepository
    # We will use the following repo to determine whether the security is held
    held_securities_repo: SecurityRepository
    # ... and then add/remove the security from the following repo
    held_securities_with_prices_repo: SecuritiesWithPricesRepository

    def handle(self, event: Union[PositionCreatedEvent, PositionDeletedEvent]):
        """ Handle the event """
        # Upsert to the provided position repo
        try:

            # Temp20230912: skip if before today
            if event.position.attributes['ts_ms'] < 1694551006580:  # 1694494394582:
                logging.info(f'Skipping position which is too early (temp20230912)')
                return True

            # Check whether the repo implements an upsert, as this is required
            if not hasattr(self.position_repo, 'upsert'):
                logging.exception(NotImplementedError(f"{self.position_repo.__class__.__name__} must provide an upsert method!"))
                logging.error('PositionEventHandler returning False as the Position event has not been processed!')
                return False

            # Check whether the repo implements remove_securities, as this is required
            if not hasattr(self.held_securities_with_prices_repo, 'remove_securities'):
                logging.exception(NotImplementedError(f"{self.held_securities_with_prices_repo.__class__.__name__} must provide a remove_securities method!"))
                logging.error('PositionEventHandler returning False as the Position event has not been processed!')
                return False
                
            # Check whether the repo implements refresh_for_securities, as this is required
            if not hasattr(self.held_securities_with_prices_repo, 'refresh_for_securities'):
                logging.exception(NotImplementedError(f"{self.held_securities_with_prices_repo.__class__.__name__} must provide a refresh_for_securities method!"))
                logging.error('PositionEventHandler returning False as the Position event has not been processed!')
                return False
                
            # Initialize
            is_deleted = True if isinstance(event, PositionDeletedEvent) else False
            today = datetime.date.today()
            position = event.position
            sec = position.security

            # Determine whether this Security is held pre-event - used later to determine whether this has changed
            # TODO_LAYERS: pms_security_id is a system-specific field, so this probably doesn't belong in the application layer
            held_res = self.held_securities_repo.get(lw_id=sec.lw_id, pms_security_id=sec.attributes['pms_security_id'])
            sec_held_before = True if len(held_res) else False            
            
            # Do the upsert and confirm row cnt
            row_cnt = self.position_repo.upsert(position, is_deleted)
            if not row_cnt:
                logging.error(f'Upserted {row_cnt} rows! PositionEventHandler returning False as the Position event has not been processed!')
                return False


            # TODO_CLEANUP: remove below once confirmed not using - inefficient solution to new/deleted held secs
            # else:
            #     # Check whether the security is held
            #     sec = position.security
            #     # TODO_LAYERS: pms_security_id is a system-specific field, so this probably doesn't belong in the application layer
            #     held_res = self.held_securities_repo.get(lw_id=sec.lw_id, pms_security_id=sec.attributes['pms_security_id'])
            #     if len(held_res) and not is_deleted:
            #         # PositionCreatedEvent, and the Security is now held -> need to refresh:
            #         # TODO_PERF: this refresh is only necessary when the security isn't already in the master read model ... so should we check first?
            #         logging.info(f'Position created for {sec.lw_id}, refreshing master read model...')
            #         self.held_securities_with_prices_repo.refresh_for_securities(data_date=today, securities=held_res)
            #     elif not len(held_res) and is_deleted:
            #         # PositionDeletedEvent, and the Security is now not held -> need to remove it:
            #         logging.info(f'Position deleted for {sec}, which is now no longer held. Deleting from master read model...')
                            
            #         # Check whether the repo implements remove_securities, as this is required
            #         if not hasattr(self.held_securities_with_prices_repo, 'remove_securities'):
            #             logging.exception(NotImplementedError(f"{self.held_securities_with_prices_repo.__class__.__name__} must provide a remove_securities method!"))
            #             logging.error('PositionEventHandler returning False as the Position event has not been processed!')
            #             return False

            #         # Now remove this security from the master read model:
            #         self.held_securities_with_prices_repo.remove_securities(data_date=today, securities=sec)
            #     return True  # commit offset


            held_res = self.held_securities_repo.get(lw_id=sec.lw_id, pms_security_id=sec.attributes['pms_security_id'])
            sec_held_after = True if len(held_res) else False

            # If the security is no longer held due to this delete event, remove it from the held SWP repo
            if (is_deleted and sec_held_before and not sec_held_after):
                # Need to confirm we have an lw_id, and if not then get it, assuming we have a pms_security_id:
                if not len(sec.lw_id):
                    sec = self.security_repo.get(pms_security_id=sec.attributes['pms_security_id'])
                logging.info(f'{sec} was held before and due to this delete is not held! Removing it from {self.held_securities_with_prices_repo.__class__.__name__}...')
                self.held_securities_with_prices_repo.remove_securities(data_date=today, securities=sec)
            
            # If the security is newly held due to this create event, refresh the held SWP repo with it
            if (not is_deleted and not sec_held_before and sec_held_after):
                # Need to confirm we have an lw_id, and if not then get it, assuming we have a pms_security_id:
                if not len(sec.lw_id):
                    sec = self.security_repo.get(pms_security_id=sec.attributes['pms_security_id'])
                logging.info(f'{sec} is newly held due to this create! Refreshing it in {self.held_securities_with_prices_repo.__class__.__name__}...')
                self.held_securities_with_prices_repo.refresh_for_securities(data_date=today, securities=sec)
            
            # If we made it here, the above all succeeded
            return True  # commit offset

            # self.position_event_publisher.publish(event)  # TODO_CLEANUP
        except Exception as ex:
            logging.exception(ex)
        return True  # commit offset

    def handle_deserialization_error(self, ex):
        """ What to do when deserialization fails """
        logging.exception(ex)
        return True  # commit offset


@dataclass
class PortfolioCreatedEventHandler(EventHandler):
    # We will upsert to the following repo
    portfolio_repo: PortfolioRepository

    def handle(self, event: PortfolioCreatedEvent):
        """ Handle the event """
        # Publish using the provided event publisher
        try:
            # Check whether the repo implements an upsert, as this is required
            if not hasattr(self.portfolio_repo, 'upsert'):
                logging.exception(NotImplementedError(f"{self.portfolio_repo.__class__.__name__} must provide an upsert method!"))
                logging.error('PortfolioCreatedEventHandler returning False as the Portfolio event has not been processed!')
                return False

            # Do the upsert and confirm row cnt
            row_cnt = self.portfolio_repo.upsert(event.portfolio)
            if not row_cnt:
                logging.error(f'Upserted {row_cnt} rows! PortfolioCreatedEventHandler returning False as the Portfolio event has not been processed!')
                return False

        except Exception as ex:
            logging.exception(ex)
        return True  # commit offset

    def handle_deserialization_error(self, ex):
        """ What to do when deserialization fails """
        logging.exception(ex)
        return True  # commit offset


