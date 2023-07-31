

# core python
import argparse
import datetime
import logging
import os
import sys

# pypi


# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_dir = src_dir.replace('C:\\', f'\\\\WS215\\c$\\', 1)  
# TODO_GOLIVE: remove above once running local files
sys.path.append(src_dir)

# native
from application.event_handlers import (
    PriceBatchCreatedEventHandler, AppraisalBatchCreatedEventHandler
    , SecurityCreatedEventHandler, PortfolioCreatedEventHandler, PositionEventHandler
)
# from infrastructure.event_handlers import KafkaEventHandler  # TODO_CLEANUP: remove when not needed

from infrastructure.event_publishers import (
    KafkaCoreDBPositionEventProducer, KafkaCoreDBPortfolioCreatedEventProducer
)
from infrastructure.event_subscribers import (
    KafkaCoreDBAppraisalBatchCreatedEventConsumer
    , KafkaCoreDBPriceBatchCreatedEventConsumer, KafkaCoreDBSecurityCreatedEventConsumer
    , KafkaAPXPortfolioEventConsumer, KafkaAPXPositionEventConsumer
)
from infrastructure.file_repositories import (
    JSONHeldSecuritiesRepository, JSONHeldSecuritiesWithPricesRepository, JSONSecurityWithPricesRepository
)
from infrastructure.sql_repositories import (
    CoreDBPriceRepository, CoreDBSecurityRepository
    , LWDBAPXAppraisalPositionRepository
    , CoreDBPriceBatchRepository, CoreDBPriceAuditEntryRepository
    , APXDBHeldSecurityRepository, APXDBPortfolioRepository
)
from infrastructure.util.config import AppConfig
from infrastructure.util.file import prepare_dated_file_path
from infrastructure.util.logging import setup_logging


def main():
    parser = argparse.ArgumentParser(description='Kafka Consumer')
    parser.add_argument(
        '--data_type', '-dt', type=str, required=True
        , choices=['price-batch', 'appraisal-batch', 'security', 'portfolio', 'position']
        , help='Type of data to consume'
    )
    parser.add_argument('--reset_offset', '-ro', action='store_true', default=False, help='Reset consumer offset to beginning')
    parser.add_argument('--log_level', '-l', type=str.upper, choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help='Log level')
    parser.add_argument('--refresh_prices', '-rp', type=str, required=False, help='Refresh prices for date, YYYYMMDD format')
    
    args = parser.parse_args()
    
    if args.data_type == 'appraisal-batch':
        kafka_consumer = KafkaCoreDBAppraisalBatchCreatedEventConsumer(
            event_handler = AppraisalBatchCreatedEventHandler(
                position_repository = LWDBAPXAppraisalPositionRepository()
                , held_securities_repository = JSONHeldSecuritiesRepository()
                , held_securities_with_prices_repository = JSONHeldSecuritiesWithPricesRepository()
            )
        )
        log_file = prepare_dated_file_path(AppConfig().get("logging", "log_dir"), datetime.date.today(), AppConfig().get("logging", "kafka_consumer_appraisal_batch_logfile"))
        setup_logging(args.log_level, log_file)
        kafka_consumer.consume(reset_offset=args.reset_offset)
    elif args.data_type == 'price-batch':
        kafka_consumer = KafkaCoreDBPriceBatchCreatedEventConsumer(
            event_handler = PriceBatchCreatedEventHandler(
                price_repository = CoreDBPriceRepository()
                , security_repository = CoreDBSecurityRepository()
                , audit_trail_repository = CoreDBPriceAuditEntryRepository()
                , security_with_prices_repository = JSONSecurityWithPricesRepository()
                , held_securities_with_prices_repository = JSONHeldSecuritiesWithPricesRepository()
            )
        )
        log_file = prepare_dated_file_path(AppConfig().get("logging", "log_dir"), datetime.date.today(), AppConfig().get("logging", "kafka_consumer_price_batch_logfile"))
        setup_logging(args.log_level, log_file)
        kafka_consumer.consume(reset_offset=args.reset_offset)
    elif args.data_type == 'security':
        kafka_consumer = KafkaCoreDBSecurityCreatedEventConsumer(
            event_handler = SecurityCreatedEventHandler(
                price_repository = CoreDBPriceRepository()
                , audit_trail_repository = CoreDBPriceAuditEntryRepository()
                , security_with_prices_repository = JSONSecurityWithPricesRepository()
                , held_securities_with_prices_repository = JSONHeldSecuritiesWithPricesRepository()
            )
        )
        log_file = prepare_dated_file_path(AppConfig().get("logging", "log_dir"), datetime.date.today(), AppConfig().get("logging", "kafka_consumer_security_logfile"))
        setup_logging(args.log_level, log_file)
        kafka_consumer.consume(reset_offset=args.reset_offset)
    elif args.data_type == 'portfolio':
        kafka_consumer = KafkaAPXPortfolioEventConsumer(
            event_handler = PortfolioCreatedEventHandler(
                portfolio_event_publisher = KafkaCoreDBPortfolioCreatedEventProducer()
            )
            , portfolio_repository = APXDBPortfolioRepository()
        )
        log_file = prepare_dated_file_path(AppConfig().get("logging", "log_dir"), datetime.date.today(), AppConfig().get("logging", "kafka_consumer_portfolio_logfile"))
        setup_logging(args.log_level, log_file)
        kafka_consumer.consume(reset_offset=args.reset_offset)
    elif args.data_type == 'position':
        kafka_consumer = KafkaAPXPositionEventConsumer(
            event_handler = PositionEventHandler(
                position_event_publisher = KafkaCoreDBPositionEventProducer()
            )
        )
        log_file = prepare_dated_file_path(AppConfig().get("logging", "log_dir"), datetime.date.today(), AppConfig().get("logging", "kafka_consumer_position_logfile"))
        setup_logging(args.log_level, log_file)
        kafka_consumer.consume(reset_offset=args.reset_offset)
    else:
        logging.error(f"Unconfigured data_type: {args.data_type}!")
        return 1



if __name__ == '__main__':
    main()
