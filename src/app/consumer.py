

# core python
import argparse
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
from infrastructure.event_handlers import KafkaEventHandler
from infrastructure.sql_repositories import CoreDBPriceRepository
from infrastructure.util.config import AppConfig


def main():
    parser = argparse.ArgumentParser(description='Kafka Consumer')
    parser.add_argument('--data_type', '-dt', type=str, required=True, choices=['price-batch', 'appraisal-batch', 'security'], help='Type of data to consume')
    parser.add_argument('--reset_offset', '-ro', type=bool, default=False, help='Reset consumer offset to beginning')
    args = parser.parse_args()
    if args.data_type == 'price-batch':
        kafka_consumer = KafkaEventHandler(
            event_class = PriceBatchCreatedEvent
        )
        event_handler = PriceBatchCreatedEventHandler(price_repository=CoreDBPriceRepository(),
            security_with_prices_repository=None, securities_with_prices_repository=None
        )
    elif args.data_type == 'appraisal-batch':
        kafka_consumer = KafkaEventHandler(
            event_class = AppraisalBatchCreatedEvent
        )
        event_handler = AppraisalBatchCreatedEventHandler()
    elif args.data_type == 'security':
        kafka_consumer = KafkaEventHandler(
            # TODO: cmd line args / config files for below, including what to consume
            config_file=args.config_file, reset_offset=args.reset_offset
            , topics=["jdbc.lwdb.coredb.pricing.vw_security"]
            , event_class = SecurityCreatedEvent
        )
        event_handler = SecurityCreatedEventHandler()
    else:
        logging.error(f"Unconfigured data_type: {args.data_type}!")
        return 1
    
    while True:
        event = kafka_consumer.get_single_event()
        event_handler.handle_price_batch_created(event)


if __name__ == '__main__':
    main()
