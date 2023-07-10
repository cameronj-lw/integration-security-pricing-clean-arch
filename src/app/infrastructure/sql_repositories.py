
# core python
import datetime
import logging
from typing import List, Optional, Tuple, Union

# pypi
import pandas as pd
from sqlalchemy import exc, update

# native
from app.application.models import ColumnConfig, UserWithColumnConfig
from app.application.repositories import UserWithColumnConfigRepository
from app.domain.models import (
    Price, Security, PriceAuditEntry, PriceBatch
    , PriceFeed, PriceFeedWithStatus, PriceSource, PriceType
    , Position
)
from app.domain.repositories import (
    SecurityRepository, PriceRepository, PriceBatchRepository
    , PriceFeedRepository, PriceFeedWithStatusRepository
    , PriceAuditEntryRepository, PriceSourceRepository, PriceTypeRepository
    , PositionRepository
)
from app.infrastructure.sql_models import (
    MGMTDBPriceFeed, MGMTDBPriceFeedWithStatus
)
from app.infrastructure.sql_tables import (
    CoreDBManualPricingSecurityTable, CoreDBColumnConfigTable
    , CoreDBvwPriceView, CoreDBvwSecurityView, CoreDBvwPriceBatchView
    , LWDBAPXAppraisalTable
)
from app.infrastructure.util.config import AppConfig
from app.infrastructure.util.date import get_current_bday, get_previous_bday
from app.infrastructure.util.dataframe import add_is_deleted, add_modified


class UnexpectedRowCountException(Exception):
    pass


class CoreDBSecurityRepository(SecurityRepository):
    def create(self, security: Security) -> Security:
        pass  # TODO: implement

    def get(self, lw_id: Union[str,None] = None) -> List[Security]:
        query_result = CoreDBvwSecurityView().read(lw_id=lw_id)
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
        logging.info(f"About to insert {df}")
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
        logging.info(f"About to insert {df}")
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
    def create(self, price: Price) -> Price:
        pass

    def get(self, data_date: datetime.date, source: Union[PriceSource,None]=None
            , security: Union[Security,None]=None) -> List[Price]:
        query_result = CoreDBvwPriceView().read(data_date=data_date, source_name=(None if source is None else source.name)
                , lw_id=(security.lw_id if security is not None else None))
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
                values[k] = qr[k]
                del qr[k]
            price_dict['values'] = values

            # Now it has the "values" in a separate item. Add to master list.
            # price_dicts.append(price_dict)
            res.append(Price.from_dict(price_dict))


        # Once all are ready, prepare the list of Prices and return
        return res  # [Price.from_dict(px) for px in price_dicts]


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

    def get(self, data_date: datetime.date, security: Security) -> List[PriceAuditEntry]:
        pass


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


