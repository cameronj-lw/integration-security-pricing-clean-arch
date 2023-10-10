

# core python
import argparse
import datetime
import logging
import os
import sys

# pypi


# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)

# native
from application.event_handlers import (
    PriceBatchCreatedEventHandler, AppraisalBatchCreatedEventHandler
    , SecurityCreatedEventHandler
    , PositionEventHandler, PortfolioCreatedEventHandler
)
from domain.events import (
    PriceBatchCreatedEvent, AppraisalBatchCreatedEvent
    , SecurityCreatedEvent
    , PositionCreatedEvent, PortfolioCreatedEvent
)

from infrastructure.event_publishers import (
    KafkaCoreDBPositionEventProducer, KafkaCoreDBPortfolioCreatedEventProducer
)
from infrastructure.event_subscribers import (KafkaCoreDBAppraisalBatchCreatedEventConsumer
    , KafkaCoreDBPriceBatchCreatedEventConsumer, KafkaCoreDBSecurityCreatedEventConsumer
)
from infrastructure.file_repositories import (
    JSONHeldSecuritiesRepository, JSONHeldSecuritiesWithPricesRepository, JSONSecurityWithPricesRepository
)
from infrastructure.sql_repositories import (
    CoreDBPriceRepository, CoreDBSecurityRepository, CoreDBPortfolioRepository
    , LWDBAPXAppraisalPositionRepository
    , CoreDBPriceBatchRepository, CoreDBPositionRepository
    , CoreDBPriceAuditEntryRepository, CoreDBLiveHeldSecurityRepository
    , APXDBLivePositionRepository, APXDBPortfolioRepository
)
from infrastructure.util.config import AppConfig
from infrastructure.util.file import prepare_dated_file_path
from infrastructure.util.logging import setup_logging


def main():
    parser = argparse.ArgumentParser(description='Kafka Consumer')
    parser.add_argument('--data_type', '-dt', type=str, required=True
        , choices=['security', 'master', 'price', 'position', 'portfolio']
        , help='Type of data to refresh')
    parser.add_argument('--log_level', '-l', type=str.upper, choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help='Log level')
    parser.add_argument('--refresh_prices', '-rp', type=str, required=False, help='Refresh prices for date, YYYYMMDD format')
    args = parser.parse_args()
    if args.data_type == 'security':
        secs = CoreDBSecurityRepository().get()
        event_handler = SecurityCreatedEventHandler(
            price_repository = CoreDBPriceRepository()
            , security_with_prices_repository = JSONSecurityWithPricesRepository()
            , audit_trail_repository = CoreDBPriceAuditEntryRepository()
            , held_securities_with_prices_repository = JSONHeldSecuritiesWithPricesRepository()
        )
        setup_logging(args.log_level)
        for sec in secs:
            # if sec.lw_id != 'COAM8365950':
            #     continue  # TODO_DEBUG: remove
            logging.info(f'Handling security: {sec}')
            event_handler.handle(SecurityCreatedEvent(sec))
    elif args.data_type == 'master':
        setup_logging(args.log_level)
        secs = CoreDBSecurityRepository().get()
        JSONHeldSecuritiesWithPricesRepository().refresh_for_securities(
            data_date=datetime.datetime.strptime(args.refresh_prices, '%Y%m%d').date(), securities=secs
        )
    elif args.data_type == 'price':
        price_batches = CoreDBPriceBatchRepository().get(
                data_date=datetime.datetime.strptime(args.refresh_prices, '%Y%m%d').date())
        event_handler = PriceBatchCreatedEventHandler(
                price_repository = CoreDBPriceRepository()
                , security_repository = CoreDBSecurityRepository()
                , audit_trail_repository = CoreDBPriceAuditEntryRepository()
                , security_with_prices_repository = JSONSecurityWithPricesRepository()
                , held_securities_with_prices_repository = JSONHeldSecuritiesWithPricesRepository()
        )
        setup_logging(args.log_level)
        logging.info(f'Processing {len(price_batches)} batches...')
        for batch in price_batches:
            logging.info(f'Handling price batch: {batch}')
            event_handler.handle(PriceBatchCreatedEvent(batch))
    elif args.data_type == 'position':
        positions = APXDBLivePositionRepository().get() 
        event_handler = PositionEventHandler(
            position_repo = CoreDBPositionRepository()
            , security_repo = CoreDBSecurityRepository()
            , held_securities_repo = CoreDBLiveHeldSecurityRepository()
            , held_securities_with_prices_repo = JSONHeldSecuritiesWithPricesRepository()
        )
        setup_logging(args.log_level)
        logging.info(f'Processing {len(positions)} positions...')
        for position in positions:
            logging.info(f'Handling position: {position}')
            event_handler.handle(PositionCreatedEvent(position))
    elif args.data_type == 'portfolio':
        portfolios = APXDBPortfolioRepository().get() 
        event_handler = PortfolioCreatedEventHandler(
            portfolio_repo = CoreDBPortfolioRepository()
        )
        setup_logging(args.log_level)
        logging.info(f'Processing {len(portfolios)} portfolios...')
        for portfolio in portfolios:
            logging.info(f'Handling portfolio: {portfolio}')
            event_handler.handle(PortfolioCreatedEvent(portfolio))
    else:
        logging.error(f"Unconfigured data_type: {args.data_type}!")
        return 1


if __name__ == '__main__':
    main()
