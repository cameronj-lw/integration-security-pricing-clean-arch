
# core python
from dataclasses import dataclass
import datetime
import logging
from typing import List, Tuple

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

from app.infrastructure.sql_tables import MGMTDBMonitorTable
from app.infrastructure.util.date import get_current_bday, get_previous_bday


@dataclass
class MGMTDBPriceFeed(PriceFeed):
    run_groups_and_names: dict = None
    security_type: str = None
    get_normal_eta: callable = None

    def __post_init__(self):
        if self.name == 'FTSE':
            self.run_groups_and_names = {
                'PENDING': None,
                'IN_PROGRESS': [
                    # FTP download:
                    ('FTP-FTSETMX_PX', ['FQCOUPON','FQFRN','FQMBS','FQMBSF','SCMHPDOM','SMIQUOTE']),
                    # Load2LWDB:
                    ('FTSETMX_PX', ['FQCOUPON','FQFRN','FQMBS','FQMBSF','SCMHPDOM','SMIQUOTE']),
                    # Load2Pricing:
                    ('FTSETMX_PX', ['PostProcess']),
                    # Load2APX:
                    ('APX-PRICELOAD', ['BOND_FTSETMX'])
                ],
                'PRICED': [
                    # Load2APX:
                    ('APX-PRICELOAD', ['BOND_FTSETMX'])
                ]
            }
            self.security_type = 'Canadian Bonds'
            self.get_normal_eta = lambda d: datetime.datetime.combine(d, datetime.time(hour=14, minute=15))
        elif self.name == 'MARKIT':
            self.run_groups_and_names = {
                'PENDING': [
                    # FTP upload:
                    ('MARKIT_PRICE', ['ISINS_SEND'])
                ],
                'IN_PROGRESS': [
                    # FTP download:
                    ('FTP-MARKIT', ['LeithWheeler_Nxxxx_Standard']),
                    # Load2LW and Load2Pricing:
                    ('MARKIT_PRICE', ['MARKIT_PRICE']),
                    # Load2APX:
                    ('APX-PRICELOAD', ['MARKIT_PRICE'])
                ],
                'PRICED': [
                    # Load2APX:
                    ('APX-PRICELOAD', ['MARKIT_PRICE'])
                ]
            }
            self.security_type = 'American Bonds'
            self.get_normal_eta = lambda d: datetime.datetime.combine(d, datetime.time(hour=13, minute=30))
        elif self.name == 'MARKIT_LOAN':
            self.run_groups_and_names = {
                'PENDING': None,
                'IN_PROGRESS': [
                    # FTP download:
                    ('FTP-MARKIT_LOAN', ['MARKIT_LOAN_ACCRUED','MARKIT_LOAN_CASH','MARKIT_LOAN_CASH_30D','MARKIT_LOAN_CONTRACT','MARKIT_LOAN_POSITION','MARKIT_LOAN_SECURITY']),
                    # Load2LW and Load2Pricing: 
                    ('MARKIT_LOAN', ['MARKIT_LOAN_PRICE']),
                    # Load2APX:
                    ('APX-PRICELOAD', ['MARKIT_LOAN_PRICE'])
                ],
                'PRICED': [
                    # Load2APX:
                    ('APX-PRICELOAD', ['MARKIT_LOAN_PRICE'])
                ]
            }
            self.security_type = 'American Loans'
            self.get_normal_eta = lambda d: datetime.datetime.combine(d, datetime.time(hour=14, minute=0))
        elif self.name == 'FUNDRUN':
            self.run_groups_and_names = {
                'PENDING': [
                    # FTP upload:
                    ('FUNDRUN', ['EQUITY_UPLOAD'])
                ],
                'IN_PROGRESS': [
                    # FTP download:
                    ('FTP-FUNDRUN_PRICE_EQ', ['FUNDRUN_PRICE_EQ']),
                    # Load2LWDB and Load2Pricing:
                    ('FUNDRUN', ['EQUITY_PRICE_MAIN']),
                    # Load2APX:
                    ('APX-PRICELOAD', ['EQUITY_FUNDRUN_MAIN'])
                ],
                'PRICED': [
                    # Load2APX:
                    ('APX-PRICELOAD', ['EQUITY_FUNDRUN_MAIN'])
                ]
            }
            self.security_type = 'All Equities (except Latin America)'
            self.get_normal_eta = lambda d: datetime.datetime.combine(d, datetime.time(hour=13, minute=45))
        elif self.name == 'FUNDRUN_LATAM':
            self.run_groups_and_names = {
                'PENDING': [
                    # FTP upload:
                    ('FUNDRUN', ['EQUITY_UPLOAD'])
                ],
                'IN_PROGRESS': [
                    # FTP download:
                    ('FTP-FUNDRUN_PRICE_EQ_LATAM', ['FUNDRUN_PRICE_EQ_LATAM']),
                    # Load2LWDB and Load2Pricing:
                    ('FUNDRUN', ['EQUITY_PRICE_LATAM']),
                    # Load2APX:
                    ('APX-PRICELOAD', ['EQUITY_FUNDRUN_LATAM'])
                ],
                'PRICED': [
                    # Load2APX:
                    ('APX-PRICELOAD', ['EQUITY_FUNDRUN_LATAM'])
                ]
            }
            self.security_type = 'Latin America Equities'
            self.get_normal_eta = lambda d: datetime.datetime.combine(d, datetime.time(hour=14, minute=0))
        elif self.name == 'BLOOMBERG':
            self.run_groups_and_names = {
                'PENDING': None,
                'IN_PROGRESS': [  # TODO: expand this to include others like equities?
                    # BB Snap:
                    ('BB-SNAP', ['BOND_PRICE', 'MBS_PRICE']),
                    # Load2Pricing:
                    ('LOADPRICE_FI', ['BOND_PRICE', 'MBS_PRICE']),
                    # Load2APX:
                    ('APX-PRICELOAD', ['BOND_BB'])
                ],
                'PRICED': [
                    # Load2APX:
                    ('APX-PRICELOAD', ['BOND_BB'])
                ]
            }
            self.security_type = 'All Instruments'
            self.get_normal_eta = lambda d: datetime.datetime.combine(d, datetime.time(hour=14, minute=30))
        else:
            raise NotImplementedError(f"Pricing feed not implemented: {self.name}")
    
    def _is_error(self, data_date: datetime.date) -> Tuple[bool, datetime.datetime]:
        res, max_ts = False, datetime.datetime.fromordinal(1)  # beginning of time
        for val in self.run_groups_and_names.values():
            if val is None:
                continue
            for (run_group, run_names) in val:
                for run_name in run_names:
                    mon = MGMTDBMonitorTable().read(scenario=MGMTDBMonitorTable().base_scenario, data_date=data_date
                                                , run_group=run_group, run_name=run_name, run_type='RUN')
                    error = mon[mon['run_status'] == 1]
                    if len(error.index):
                        res = True
                        max_ts = max(max_ts, error['asofdate'].max())
        return res, (max_ts if res else datetime.datetime.now())
        
    def _is_priced(self, data_date: datetime.date) -> Tuple[bool, datetime.datetime]:
        max_ts = datetime.datetime.fromordinal(1)  # beginning of time
        for (run_group, run_names) in self.run_groups_and_names['PRICED']:
            for run_name in run_names:
                mon = MGMTDBMonitorTable().read(scenario=MGMTDBMonitorTable().base_scenario, data_date=data_date
                                            , run_group=run_group, run_name=run_name, run_type='RUN')
                complete = mon[mon['run_status'] == 0]
                if not len(complete.index):
                    return False, datetime.datetime.now()
                max_ts = max(max_ts, complete['asofdate'].max())
        return True, max_ts

    def _is_in_progress(self, data_date: datetime.date) -> Tuple[bool, datetime.datetime]:
        res, max_ts = False, datetime.datetime.fromordinal(1)  # beginning of time
        for (run_group, run_names) in self.run_groups_and_names['IN_PROGRESS']:
            for run_name in run_names:
                mon = MGMTDBMonitorTable().read(scenario=MGMTDBMonitorTable().base_scenario, data_date=data_date
                                            , run_group=run_group, run_name=run_name, run_type='RUN')
                in_progress = mon[mon['run_status'] == (-1 | 0)]  # include "success" here in case some are success and others not started
                if len(in_progress.index):
                    res = True
                    max_ts = max(max_ts, in_progress['asofdate'].max())
        return res, (max_ts if res else datetime.datetime.now())

    def _is_pending(self, data_date: datetime.date) -> Tuple[bool, datetime.datetime]:
        max_ts = datetime.datetime.fromordinal(1)  # beginning of time
        # A feed may not have items indicating "Pending" status. If it doesn't, return False:
        if self.run_groups_and_names['PENDING'] is None:
            return False, datetime.datetime.now()
        for (run_group, run_names) in self.run_groups_and_names['PENDING']:
            for run_name in run_names:
                mon = MGMTDBMonitorTable().read(scenario=MGMTDBMonitorTable().base_scenario, data_date=data_date
                                            , run_group=run_group, run_name=run_name, run_type='RUN')
                complete = mon[mon['run_status'] == 0]
                if not len(complete.index):
                    return False, datetime.datetime.now()
                max_ts = max(max_ts, complete['asofdate'].max())
        return True, max_ts

    def _is_delayed(self, data_date: datetime.date) -> Tuple[bool, datetime.datetime]:
        if self._is_priced(data_date):
            return False, datetime.datetime.now()
        if self.get_normal_eta(data_date) < datetime.datetime.now():
            return True, datetime.datetime.now()
        return False, datetime.datetime.now()

    def get_status(self, data_date: datetime.date) -> Tuple[str, datetime.datetime]:
        try:
            error = self._is_error(data_date)
            if error[0]:
                return 'ERROR', error[1]
            priced = self._is_priced(data_date)
            if priced[0]:
                return 'PRICED', priced[1]
            delayed = self._is_delayed(data_date)
            if delayed[0]:
                return 'DELAYED', delayed[1]
            in_progress = self._is_in_progress(data_date)
            if in_progress[0]:
                return 'IN_PROGRESS', in_progress[1]
            pending = self._is_pending(data_date)
            if pending[0]:
                return 'PENDING', pending[1]
            # If we reached this point, there is no status... but we still want all feeds included in results
            prev_bday_priced = self._is_priced(get_previous_bday(data_date))
            if prev_bday_priced[0]:
                # provide the prev bday "priced" time if there is no status yet:
                return '-', prev_bday_priced[1]
            else:
                return '-', datetime.datetime.now()
        except exc.SQLAlchemyError as e:
            logging.exception(f"SQLAlchemy error: {e}")
            return 'EXCEPTION', datetime.datetime.now()
        except TypeError as e:
            logging.exception(f"{self.__class__.__name__} {self.name} has a wrong type in its run_groups_and_names: {e}")
            return 'EXCEPTION', datetime.datetime.now()
        except KeyError as e:
            logging.exception(f"{self.__class__.__name__} {self.name} is missing a required key: {e}")
            return 'EXCEPTION', datetime.datetime.now()


class MGMTDBPriceFeedWithStatus(PriceFeedWithStatus):

    def __post_init__(self):
        self.update_status()
        self.price_feed_class = MGMTDBPriceFeed

    def update_status(self):
        self.status, self.status_ts = self.feed.get_status(self.data_date)

    # def to_dict(self):  # TODO_CLEANUP: remove when not needed
    #     return {
    #         'data_date': self.data_date.isoformat(),
    #         'feed_name': self.feed.name,
    #         'feed_status': self.status,
    #         'last_update_ts': self.status_ts.isoformat(),
    #         'normal_eta': self.feed.get_normal_eta(self.data_date).isoformat(),
    #         'security_type': self.feed.security_type,
    #     }
            # 'data_date': self.data_date.isoformat(),
            # 'feed': self.feed.name,
            # 'status': self.status,
            # 'asofdate': self.status_ts,#.isoformat(),


