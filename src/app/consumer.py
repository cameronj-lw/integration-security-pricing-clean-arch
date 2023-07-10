

# core python
import argparse
import datetime
import logging

# pypi


# native
from application.event_handlers import (
    PriceBatchCreatedEventHandler, AppraisalBatchCreatedEventHandler
    , SecurityCreatedEventHandler
)
from domain.events import (
    PriceBatchCreatedEvent, AppraisalBatchCreatedEvent, SecurityCreatedEvent
)
# from infrastructure.event_handlers import KafkaEventHandler  # TODO_CLEANUP: remove when not needed
from infrastructure.event_subscribers import (KafkaCoreDBAppraisalBatchCreatedEventConsumer
    , KafkaCoreDBPriceBatchCreatedEventConsumer, KafkaCoreDBSecurityCreatedEventConsumer
)
from infrastructure.file_repositories import (
    JSONHeldSecuritiesRepository, JSONHeldSecuritiesWithPricesRepository, JSONSecurityWithPricesRepository
)
from infrastructure.sql_repositories import (
    CoreDBPriceRepository, CoreDBSecurityRepository
    , LWDBAPXAppraisalPositionRepository
    , CoreDBPriceBatchRepository
)
from infrastructure.util.config import AppConfig
from infrastructure.util.file import prepare_dated_file_path
from infrastructure.util.logging import setup_logging


def main():
    parser = argparse.ArgumentParser(description='Kafka Consumer')
    parser.add_argument('--data_type', '-dt', type=str, required=True
        , choices=['price-batch', 'appraisal-batch', 'security', 'security-refresh', 'price-refresh'], help='Type of data to consume')
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
        kafka_consumer.consume()
    elif args.data_type == 'price-batch':
        kafka_consumer = KafkaCoreDBPriceBatchCreatedEventConsumer(
            event_handler = PriceBatchCreatedEventHandler(
                price_repository = CoreDBPriceRepository()
                , security_with_prices_repository = JSONSecurityWithPricesRepository()
                , held_securities_with_prices_repository = JSONHeldSecuritiesWithPricesRepository()
            )
        )
        log_file = prepare_dated_file_path(AppConfig().get("logging", "log_dir"), datetime.date.today(), AppConfig().get("logging", "kafka_consumer_price_batch_logfile"))
        setup_logging(args.log_level, log_file)
        kafka_consumer.consume()
    elif args.data_type == 'security':
        kafka_consumer = KafkaCoreDBSecurityCreatedEventConsumer(
            event_handler = SecurityCreatedEventHandler(
                security_with_prices_repository = JSONSecurityWithPricesRepository()
                , held_securities_with_prices_repository = JSONHeldSecuritiesWithPricesRepository()
            )
        )
        log_file = prepare_dated_file_path(AppConfig().get("logging", "log_dir"), datetime.date.today(), AppConfig().get("logging", "kafka_consumer_security_logfile"))
        setup_logging(args.log_level, log_file)
        kafka_consumer.consume()
    elif args.data_type == 'security-refresh':
        secs = CoreDBSecurityRepository().get()
        event_handler = SecurityCreatedEventHandler(
            security_with_prices_repository = JSONSecurityWithPricesRepository()
            , held_securities_with_prices_repository = JSONHeldSecuritiesWithPricesRepository()
        )
        setup_logging(args.log_level)
        for sec in secs:
            # if sec.lw_id != 'COAM8365950':
            #     continue  # TODO_DEBUG: remove
            logging.info(f'Handling security: {sec}')
            event_handler.handle(SecurityCreatedEvent(sec))
    elif args.data_type == 'price-refresh':
        price_batches = CoreDBPriceBatchRepository().get(
                data_date=datetime.datetime.strptime(args.refresh_prices, '%Y%m%d').date())
        event_handler = PriceBatchCreatedEventHandler(
            price_repository = CoreDBPriceRepository()
            , security_with_prices_repository = JSONSecurityWithPricesRepository()
            , held_securities_with_prices_repository = JSONHeldSecuritiesWithPricesRepository()
        )
        setup_logging(args.log_level)
        for batch in price_batches:
            logging.info(f'Handling price batch: {batch}')
            event_handler.handle(PriceBatchCreatedEvent(batch))
    else:
        logging.error(f"Unconfigured data_type: {args.data_type}!")
        return 1



if __name__ == '__main__':
    main()
