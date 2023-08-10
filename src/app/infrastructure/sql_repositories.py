
# core python
import copy
import datetime
import logging
import os
import socket
from typing import List, Optional, Tuple, Union

# pypi
import numpy as np
import pandas as pd
from sqlalchemy import exc, update, and_

# native
from app.application.models import ColumnConfig, UserWithColumnConfig
from app.application.repositories import UserWithColumnConfigRepository
from app.domain.models import (
    Price, Security, PriceAuditEntry, PriceBatch
    , PriceFeed, PriceFeedWithStatus, PriceSource, PriceType
    , Position, PriceValue, SecurityWithPrices, Portfolio
)
from app.domain.repositories import (
    SecurityRepository, PriceRepository, PriceBatchRepository
    , PriceFeedRepository, PriceFeedWithStatusRepository
    , PriceAuditEntryRepository, PriceSourceRepository, PriceTypeRepository
    , PositionRepository, SecuritiesForDateRepository, SecurityWithPricesRepository
    , PortfolioRepository
)
from app.infrastructure.sql_models import (
    MGMTDBPriceFeed, MGMTDBPriceFeedWithStatus
)
from app.infrastructure.sql_tables import (
    CoreDBManualPricingSecurityTable, CoreDBColumnConfigTable, CoreDBPriceAuditEntryTable
    , CoreDBvwPriceView, CoreDBvwSecurityView, CoreDBvwPriceBatchView, CoreDBvwHeldSecurityByDateView
    , CoreDBvwHeldSecurityView, CoreDBvwAPXAppraisalView
    , LWDBAPXAppraisalTable, LWDBPricingTable, CoreDBvwPortfolioView
    , APXDBvQbRowDefPositionView, APXDBvPortfolioView, APXDBAdvPositionTable
)
from app.infrastructure.util.config import AppConfig
from app.infrastructure.util.date import get_current_bday, get_previous_bday
from app.infrastructure.util.dataframe import add_is_deleted, add_modified


class UnexpectedRowCountException(Exception):
    pass


class CoreDBSecurityRepository(SecurityRepository):
    def create(self, security: Security) -> Security:
        pass  # TODO: implement

    def get(self, lw_id: Union[List[str],str,None] = None
            , pms_security_id: Union[List[int],int,None] = None) -> List[Security]:
        query_result = CoreDBvwSecurityView().read(
            lw_id=lw_id if isinstance(lw_id, str) else None
            , pms_security_id=pms_security_id if isinstance(pms_security_id, int) else None
        )

        # If 0 rows, return empty list
        if not len(query_result.index):
            return []
        
        # Trim to securities, if applicable
        if isinstance(lw_id, str):
            query_result = query_result[query_result['lw_id'].equals(lw_id)]
        elif isinstance(lw_id, list):
            query_result = query_result[query_result['lw_id'].isin(lw_id)]
        if isinstance(pms_security_id, int):
            logging.info(f'Filtering {len(query_result.index)} secs {query_result.columns} for PMS sec ID {pms_security_id}')
            query_result = query_result[query_result['pms_security_id'].equals(pms_security_id)]
        elif isinstance(pms_security_id, list):
            query_result = query_result[query_result['pms_security_id'].isin(pms_security_id)]

        # query result should be a DataFrame. Need to convert to list of Securities:
        secs = [Security(sec['lw_id'], sec) for sec in query_result.to_dict('records')]
        return secs


class CoreDBManualPricingSecurityRepository(SecurityRepository):
    def create(self, security: Union[Security, List[Security]]) -> int:
        # If a single sec is provided, turn into a list
        if isinstance(security, Security):
            secs = [security]
        else:
            secs = security
        # exclude any secs which are already in the repo, to avoid creating them as dupes
        secs = [s for s in secs if s not in self.get()]
        data = [{'lw_id': s.lw_id} for s in secs]
        # Convert to df and supplement with standard cols:
        df = pd.DataFrame(data)
        df = add_is_deleted(df)
        df = add_modified(df)
        logging.debug(f"About to insert {df}")
        res = CoreDBManualPricingSecurityTable().bulk_insert(df)  # TODO: error handling?
        if isinstance(res, int):
            row_cnt = res
        else:
            row_cnt = res.rowcount
        if row_cnt != len(secs):
            raise UnexpectedRowCountException(f"Expected {len(secs)} rows to be saved, but there were {row_cnt}!")
        return row_cnt

    def get(self, exclude_deleted=True) -> List[Security]:
        query_result = CoreDBManualPricingSecurityTable().read(exclude_deleted)
        # query result should be a DataFrame. Need to convert to list of Securities:
        secs = [Security(lw_id) for lw_id in query_result['lw_id'].to_list()]
        return secs

    def delete(self, security: Union[Security, List[Security]]) -> int:
        # If a single sec is provided, turn into a list
        if isinstance(security, Security):
            secs = [security]
        else:
            secs = security
        # exclude any secs which are not currently in the repo, since we don't need to delete them:
        secs = [s for s in secs if s in self.get()]
        lw_ids = [s.lw_id for s in secs]
        mps_table = CoreDBManualPricingSecurityTable()
        new_vals = {'is_deleted': True}
        new_vals = add_modified(new_vals)
        stmt = update(mps_table.table_def).values(new_vals)
        # Update only for the provided lw_id's, and not already deleted:
        stmt = stmt.where(mps_table.c.is_deleted == False)
        stmt = stmt.filter(mps_table.c.lw_id.in_(lw_ids))
        row_cnt = mps_table._database.execute_write(stmt
            , commit=AppConfig().parser.get('app', 'commit', fallback=False)).rowcount
        return row_cnt


class CoreDBColumnConfigRepository(UserWithColumnConfigRepository):
    def create(self, user_with_column_config: UserWithColumnConfig) -> int:
        data = [{'user_id': user_with_column_config.user_id, 'column_name': u.column_name, 'is_hidden': u.is_hidden} 
            for u in user_with_column_config.column_configs]
        # Convert to df and supplement with standard cols:
        df = pd.DataFrame(data)
        df = add_is_deleted(df)
        df = add_modified(df)
        logging.debug(f"About to insert {df}")
        res = CoreDBColumnConfigTable().bulk_insert(df)  # TODO: error handling?
        if isinstance(res, int):
            row_cnt = res
        else:
            row_cnt = res.rowcount
        if row_cnt != len(user_with_column_config.column_configs):
            raise UnexpectedRowCountException(f"Expected {len(user_with_column_config.column_configs)} rows to be saved, but there were {row_cnt}!")
        return row_cnt

    def get(self, user_id: str, exclude_deleted=True) -> UserWithColumnConfig:
        query_result = CoreDBColumnConfigTable().read(user_id)
        # query result should be a DataFrame. Need to convert to list of ColumnConfigs and then create UserWithColumnConfig:
        column_configs = [ColumnConfig(cc['column_name'], cc['is_hidden']) for cc in query_result.to_dict('records')]
        user_with_column_config = UserWithColumnConfig(user_id, column_configs)
        return user_with_column_config

    def delete(self, user_id: str) -> int:
        cc_table = CoreDBColumnConfigTable()
        new_vals = {'is_deleted': True}
        new_vals = add_modified(new_vals)
        stmt = update(cc_table.table_def).values(new_vals)
        # Update only for the provided user_id, and not already deleted:
        stmt = stmt.where(cc_table.c.user_id == user_id)
        stmt = stmt.where(cc_table.c.is_deleted == False)
        row_cnt = cc_table._database.execute_write(stmt
            , commit=AppConfig().parser.get('app', 'commit', fallback=False)).rowcount
        return row_cnt


class CoreDBPriceRepository(PriceRepository):
    def create(self, prices: Union[List[Price], Price]) -> Union[List[Price], Price]:
        pass

    def get(self, data_date: datetime.date, source: Union[PriceSource,None]=None
            , security: Union[List[Security],Security,None]=None) -> List[Price]:
        query_result = CoreDBvwPriceView().read(data_date=data_date, source_name=(None if source is None else source.name))

        # Trim to securities, if applicable
        if isinstance(security, Security):
            query_result = query_result[query_result['lw_id'].equals(security.lw_id)]
        elif isinstance(security, list):
            lw_ids = [sec.lw_id for sec in security]
            query_result = query_result[query_result['lw_id'].isin(lw_ids)]

        query_result_dicts = query_result.to_dict('records')

        res = []
        
        for qr in query_result_dicts:
            logging.debug(f'Processing price query result: {qr}')
            price_dict = qr.copy()
            
            # Need to put price/yield/duration into a separate "values" item:
            values = {}
            for k in ('price', 'yield', 'duration'):
                if k not in price_dict:
                    continue
                values[k] = None if qr[k] in (None, np.nan) else qr[k]
                del qr[k]
            price_dict['values'] = values

            # Now it has the "values" in a separate item. Add to master list.
            # price_dicts.append(price_dict)
            res.append(Price.from_dict(price_dict))


        # Once all are ready, prepare the list of Prices and return
        return res  # [Price.from_dict(px) for px in price_dicts]


class LWDBPriceRepository(PriceRepository):
    def create(self, prices: Union[List[Price], Price]) -> Union[List[Price], Price]:
        if isinstance(prices, Price):
            # Need to convert to List in order to loop thru
            _prices = copy.deepcopy(prices)
        else:
            _prices = [copy.deepcopy(px) for px in prices]
                
        # Rotate scenario
        table = LWDBPricingTable()
        table.base_scenario = AppConfig().get('lwdb', 'pricing_base_scenario', fallback='LW_SEC_PRICING')
        
        for px in _prices:
            # We want to rotate for each source and security combo separately.
            # By doing this, if there are other securities for a given source, they will remain intact,
            # rather than having their scenario rotated when there is no new row to take their place as the base scenario.
            
            # Add LW_ prefix if manual/override:
            logging.info(f'Mid LWDB create 1: {prices}')
            if px.source.name in ('MANUAL', 'OVERRIDE'):
                px.source = PriceSource('LW_' + px.source.name)
            logging.info(f'Mid LWDB create 2: {prices}')
            extra_where = and_(
                table.table_def.c.source == px.source.name,
                table.table_def.c.lw_id == px.security.lw_id
            )

            # Now rotate
            table.rotate(data_date=px.data_date, extra_where=extra_where
                    , commit=AppConfig().parser.get('app', 'commit', fallback=False))

        # Build master list of dicts to populate the DataFrame later for the insert
        prices_dicts = [px.to_dict() for px in _prices]

        # Create df to bulk insert
        prices_df = pd.DataFrame(data=prices_dicts)

        # Add standard columns
        prices_df['scenario'] = table.base_scenario
        prices_df['scenariodate'] = prices_df['asofdate'] = datetime.datetime.now().isoformat()[:-3]
        prices_df['asofuser'] = f"{os.getlogin()}_{socket.gethostname()}"

        # Translate to match table column names
        if 'data_date' in prices_df.columns and 'data_dt' not in prices_df.columns:
            prices_df['data_dt'] = prices_df['data_date']
            prices_df = prices_df.drop(['data_date'], axis=1)
        if 'duration' in prices_df.columns and 'moddur' not in prices_df.columns:
            prices_df['moddur'] = prices_df['duration']
            prices_df = prices_df.drop(['duration'], axis=1)

        # Drop these since they DNE in pricing table
        if 'modified_at' in prices_df.columns:
            prices_df = prices_df.drop(['modified_at'], axis=1)
        # Bulk insert new rows:
        logging.debug(f"About to insert {prices_df}")
        res = table.bulk_insert(prices_df)  # TODO: error handling?
        if isinstance(res, int):
            row_cnt = res
        else:
            row_cnt = res.rowcount
        if row_cnt != len(_prices):
            raise UnexpectedRowCountException(f"Expected {len(_prices)} rows to be saved, but there were {row_cnt}!")
        logging.info(f'End of LWDB create: {prices}')
        return row_cnt

    def get(self, data_date: datetime.date, source: Union[PriceSource,None]=None
            , security: Union[Security,None]=None) -> List[Price]:
        pass


class CoreDBPriceBatchRepository(PriceBatchRepository):
    def create(self, price: PriceBatch) -> PriceBatch:
        pass

    def get(self, data_date: Union[datetime.date,None]=None
            , source: Union[PriceSource,None]=None) -> List[PriceBatch]:
        # Query from DB, get results into list of dicts
        query_result = CoreDBvwPriceBatchView().read(data_date=data_date, source_name=(None if source is None else source.name))
        query_result_dicts = query_result.to_dict('records')

        # Create list of PriceBatches
        batches = [PriceBatch.from_dict(qrd) for qrd in query_result_dicts]

        # Translate price sources and return
        for pb in batches:
            pass  # pb.source = self.translate_price_source(pb.source)
        return batches

    def translate_price_source(self, source: PriceSource) -> PriceSource:
        """ Translate the price source from the batch into a more generic source
        
        Args:
        - source (PriceSource): Source to translate.

        Returns:
        - PriceSource: Translated source.
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


class LWDBAPXAppraisalPositionRepository(PositionRepository):
    def create(self, position: Position) -> Position:
        pass  # Not needed to implement here, we think...

    def get(self, data_date: datetime.date) -> List[Position]:  # TODO: support portfolio and/or security?
        query_result = LWDBAPXAppraisalTable().read_for_date(data_date)
        positions = [Position(pos['Portfolios'], data_date, Security(pos['ProprietarySymbol'])
                , []  # TODO: actually add prices here?
            ) for pos in query_result.to_dict('records')]
        return positions

    # TODO_CLEANUP: remove when not needed
    def get_unique_securities(self, data_date: datetime.date) -> List[Security]:
        positions = self.get(data_date)
        lw_ids = [pos.security.lw_id for pos in positions]
        unique_lw_ids = set(lw_ids)
        unique_secs = [Security(lw_id) for lw_id in unique_lw_ids]
        return unique_secs


class MGMTDBPriceFeedRepository(PriceFeedRepository):  # TODO: is this needed?
    def create(self, price_feed: MGMTDBPriceFeed) -> MGMTDBPriceFeed:
        pass

    def get(self, name: str) -> List[PriceFeed]:
        pass


class MGMTDBPriceFeedWithStatusRepository(PriceFeedWithStatusRepository):
    price_feed_class = MGMTDBPriceFeed

    def create(self, price_feed_with_status: PriceFeedWithStatus) -> PriceFeedWithStatus:
        pass  # TODO: implement

    def get(self, data_date: datetime.date, feeds: List[PriceFeed]) -> List[PriceFeedWithStatus]:
        result = []
        if data_date >= datetime.date(2011, 1, 1):
            data_date = get_current_bday(data_date)
        for feed in feeds:
            feed_with_status = MGMTDBPriceFeedWithStatus(feed, data_date)
            feed_with_status.update_status()
            result.append(feed_with_status)
        return result

    # def to_dict(self, feeds_with_statuses: List[MGMTDBPriceFeedWithStatus]) -> List[dict]:
        # TODO_CLEANUP: remove when not needed... 
        # result = {}
        # for fr in feeds_with_statuses:
        #     result[fr.feed.name] = {
        #         'status': fr.status,
        #         'asofdate': fr.status_ts.isoformat(),
        #         'normal_eta': fr.feed.get_normal_eta(fr.data_date).isoformat(),
        #         'security_type': fr.feed.security_type,
        #     }
        # return result


class CoreDBPriceAuditEntryRepository(PriceAuditEntryRepository):
    def create(self, price_audit_entry: PriceAuditEntry) -> PriceAuditEntry:
        pass

    def get(self, data_date: Union[datetime.date,None]=None, security: Union[List[Security],Security,None]=None) -> List[PriceAuditEntry]:
        query_result = CoreDBPriceAuditEntryTable().read(data_date)
        
        # Trim to securities, if applicable
        if isinstance(security, Security):
            query_result = query_result[query_result['lw_id'].equals(security.lw_id)]
        elif isinstance(security, list):
            lw_ids = [sec.lw_id for sec in security]
            query_result = query_result[query_result['lw_id'].isin(lw_ids)]

        audit_entries = []
        for qr_dict in query_result.to_dict('records'):
            # Build PriceAuditEntry from this row in query result

            # Find keys of format "xyz_before" or "xyz_after", 
            # where xyz is either "source" or the name of the PriceType:
            before_fields_dict = {k:v for k,v in qr_dict.items() if k[-7:] == '_before'}
            after_fields_dict = {k:v for k,v in qr_dict.items() if k[-6:] == '_after'}

            # Get price sources - also remove them from dicts since they are the only field 
            # not representing a price type (e.g. price/yield/duration)
            before_source_name = before_fields_dict.pop('source_before')  # TODO: catch KeyError exception here?
            after_source_name = after_fields_dict.pop('source_after')  # TODO: catch KeyError exception here?

            # Create Prices for before & after, based on the discovered fields
            before_price = Price(security=Security(qr_dict['lw_id']), 
                    source=PriceSource(before_source_name),
                    data_date=qr_dict['data_date'],
                    modified_at=qr_dict['modified_at'],
                    values=[PriceValue(PriceType(k[:-7]), v) for k,v in before_fields_dict.items()]
            )
            after_price = Price(security=Security(qr_dict['lw_id']), 
                    source=PriceSource(after_source_name),
                    data_date=qr_dict['data_date'],
                    modified_at=qr_dict['modified_at'],
                    values=[PriceValue(PriceType(k[:-6]), v) for k,v in after_fields_dict.items()]
            )

            # Create PriceAuditEntry instance and add to results
            ae = PriceAuditEntry(data_date=qr_dict['data_date'], security=Security(qr_dict['lw_id']),
                    reason=qr_dict['reason'], comment=qr_dict['comment'],
                    before=before_price, after=after_price,
                    modified_by=qr_dict['modified_by'], modified_at=qr_dict['modified_at']
            )
            audit_entries.append(ae)
        logging.debug(f'SQL repo returning {audit_entries}')
        return audit_entries


class CoreDBSecurityWithPricesRepository(SecurityWithPricesRepository):
    # TODO_LAYERS: re-evaluate where this belongs? 
    # e.g. pulling 'PXAPX' prices specifically for prev bday prices is more an application layer decision

    def create(self, security_with_prices: SecurityWithPrices) -> SecurityWithPrices:
        pass

    def add_price(self, price: Price, mode='curr') -> SecurityWithPrices:
        pass

    def add_security(self, data_date: datetime.date, security: Security) -> SecurityWithPrices:
        pass

    def get(self, data_date: datetime.date, security: Union[List[Security],Security,None]=None) -> List[SecurityWithPrices]:
        # Initialize - make into list if a single security is requested
        requested_secs = [security] if isinstance(security, Security) else security

        # Get securities
        if requested_secs is None:
            secs = CoreDBSecurityRepository().get(lw_id=None)
        else:
            secs = CoreDBSecurityRepository().get(lw_id=[sec.lw_id for sec in requested_secs])

        # Get prev bday prices
        prev_bday_prices = CoreDBPriceRepository().get(data_date=get_previous_bday(data_date)
                , source=PriceSource('PXAPX'), security=secs)

        # Get curr bday prices
        curr_bday_prices = CoreDBPriceRepository().get(data_date=data_date, security=secs)
        # Update source as translated source
        for i, px in enumerate(curr_bday_prices):
            px.source = self.translate_price_source(px.source)
            curr_bday_prices[i] = px
        # Filter to relevant prices
        curr_bday_prices = [px for px in curr_bday_prices if self.feed_is_relevant(px.source)]

        # Get audit trail
        audit_trails = CoreDBPriceAuditEntryRepository().get(data_date=data_date, security=secs)

        # Will append to results below
        securities_with_prices = []
        for sec in secs:
            # Filter prices & audit trail for this security
            sec_prev_bday_prices = [px for px in prev_bday_prices if px.security.lw_id == sec.lw_id]
            sec_prev_bday_price = sec_prev_bday_prices[0] if len(sec_prev_bday_prices) else None
            sec_curr_bday_prices = [px for px in curr_bday_prices if px.security.lw_id == sec.lw_id]
            sec_audit_trail = [at for at in audit_trails if at.security.lw_id == sec.lw_id]

            # Create SWP instance
            swp = SecurityWithPrices(
                    security=sec, data_date=data_date, curr_bday_prices=sec_curr_bday_prices
                    , prev_bday_price=sec_prev_bday_price, audit_trail=sec_audit_trail
            )

            # Append it to master results
            securities_with_prices.append(swp)

        logging.info(f'CoreDBSecurityWithPricesRepository GET returning {len(securities_with_prices)} SWPs')
        return securities_with_prices


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


class CoreDBHeldSecurityRepository(SecuritiesForDateRepository):
    def create(self, data_date: datetime.date, security: Union[Security, List[Security]]) -> int:
        pass  # Not needed to implement here, we think...

    def get(self, data_date: datetime.date, security: Union[Security, None] = None) -> List[Security]:
        if security is not None:
            raise NotImplementedError(f'CoreDBHeldSecurityRepository GET does not support querying for individual securities.')
        query_result = CoreDBvwAPXAppraisalView().read_distinct_securities(data_date=data_date)

        # If there is a query result, we'll use it:
        if len(query_result.index):
            secs = [Security(sec['lw_id'], {'pms_security_id': sec['pms_security_id']}) for sec in query_result.to_dict('records')]
        else:  # If not, we'll query the CoreDB live positions view and use that:
            query_result = CoreDBvwHeldSecurityView().read()
            secs = [Security(sec['lw_id'], sec) for sec in query_result.to_dict('records')]
        
        # Return resulting Securities list
        return secs


class CoreDBPortfolioRepository(PortfolioRepository):
    def create(self, portfolio: Portfolio) -> Portfolio:
        pass  # Not needed to implement here, we think...

    def get(self, portfolio_code: Union[str,None]=None, pms_portfolio_id: Union[int,None]=None) -> List[Portfolio]:
        query_result = CoreDBvwPortfolioView().read(portfolio_code=portfolio_code, pms_portfolio_id=pms_portfolio_id)
        
        # query result should be a DataFrame. Need to convert to list of Securities:
        portfs = [Portfolio(portf['portfolio_code'], portf) for portf in query_result.to_dict('records')]
        return portfs


class CoreDBPriceSourceRepository(PriceSourceRepository):
    def create(self, price_source: PriceSource) -> PriceSource:
        pass

    def get(self, name: str) -> List[PriceSource]:
        pass


class APXDBPriceTypeRepository(PriceTypeRepository):
    def create(self, price_type: PriceType) -> PriceType:
        pass

    def get(self, name: str) -> List[PriceType]:
        pass


class APXDBHeldSecurityRepository(SecurityRepository):
    def create(self, security: Union[Security, List[Security]]) -> int:
        pass

    def get(self, lw_id: Union[List[str],str,None] = None) -> List[Security]:
        print(f'Getting apx positions...')
        apx_positions = APXDBvQbRowDefPositionView().read()
        print(f'Getting securities...')
        securities = CoreDBSecurityRepository().get()
        print(f'Mapping...')
        held_secs = [sec for sec in securities if sec.attributes['pms_security_id'] in apx_positions['SecurityID']]
        return held_secs


class APXDBLivePositionRepository(PositionRepository):
    def create(self, position: Position) -> Position:
        pass

    def get(self, data_date: Union[datetime.date,None]=None, security: Union[Security,None]=None
                , portfolio: Union[Portfolio,None]=None) -> List[Position]:
        query_result = APXDBAdvPositionTable().read()
        
        # query result should be a DataFrame. Need to convert to list of Positions:
        res = []
        for pos_dict in query_result.to_dict('records'):

            # Add pms_position_id (LW naming convention)
            pos_dict.update({'pms_position_id': pos_dict['PositionID']})

            # Create Position instance, append to results
            position = Position(
                portfolio=Portfolio(portfolio_code='', attributes={'pms_portfolio_id': pos_dict['PortfolioID']})
                , data_date=datetime.date.today()
                , security=Security(lw_id='', attributes={'pms_security_id': pos_dict['SecurityID']})
                , quantity=pos_dict['Quantity'], is_short=pos_dict['IsShortPosition']
                , attributes=pos_dict
            )
            res.append(position)

        # Finally, return
        return res


class APXDBPortfolioRepository(PortfolioRepository):
    def create(self, portfolio: Portfolio) -> Portfolio:
        pass  # Not needed until there's an app creating portfolios in APX to use this

    def get(self, portfolio_id: Union[int,None]=None) -> List[Portfolio]:
        query_result = APXDBvPortfolioView().read(portfolio_id=portfolio_id)

        # Loop thru, add some LW preferred column names, then append to results
        res = []
        for portf_dict in query_result.to_dict('records'):
            logging.info(f"Processing portfolio dict {portf_dict}")

            # Add LW naming convention keys
            portf_dict.update({
                'portfolio_code': portf_dict['PortfolioCode']
                , 'pms_portfolio_id': portf_dict['PortfolioID']
                , 'portfolio_type': portf_dict['PortfolioTypeCode']
            })

            # Create Portfolio instance and append to results
            portfolio = Portfolio(portf_dict['portfolio_code'], portf_dict)
            res.append(portfolio)
        
        # Finally, return
        return res



