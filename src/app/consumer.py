

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
# from infrastructure.event_handlers import KafkaEventHandler  # TODO_CLEANUP: remove when not needed
from infrastructure.event_subscribers import (KafkaCoreDBAppraisalBatchCreatedEventConsumer
    , KafkaCoreDBSecurityCreatedEventConsumer
)
from infrastructure.file_repositories import (
    JSONHeldSecuritiesRepository, JSONHeldSecuritiesWithPricesRepository, JSONSecurityWithPricesRepository
)
from infrastructure.sql_repositories import CoreDBPriceRepository, LWDBAPXAppraisalPositionRepository
from infrastructure.util.config import AppConfig


def main():
    parser = argparse.ArgumentParser(description='Kafka Consumer')
    parser.add_argument('--data_type', '-dt', type=str, required=True, choices=['price-batch', 'appraisal-batch', 'security'], help='Type of data to consume')
    parser.add_argument('--reset_offset', '-ro', type=bool, default=False, help='Reset consumer offset to beginning')
    args = parser.parse_args()
    if args.data_type == 'appraisal-batch':
        kafka_consumer = KafkaCoreDBAppraisalBatchCreatedEventConsumer(
            event_handler = AppraisalBatchCreatedEventHandler(
                position_repository = LWDBAPXAppraisalPositionRepository()
                , held_securities_repository = JSONHeldSecuritiesRepository()
                , held_securities_with_prices_repository = JSONHeldSecuritiesWithPricesRepository()
            )
        )
        kafka_consumer.consume()
    # elif args.data_type == 'price-batch':
    #     kafka_consumer = KafkaEventHandler(
    #         event_class = PriceBatchCreatedEvent
    #     )
    #     event_handler = PriceBatchCreatedEventHandler(price_repository=CoreDBPriceRepository(),
    #         security_with_prices_repository=None, securities_with_prices_repository=None
    #     )
    elif args.data_type == 'security':
        kafka_consumer = KafkaCoreDBSecurityCreatedEventConsumer(
            event_handler = SecurityCreatedEventHandler(
                security_with_prices_repository = JSONSecurityWithPricesRepository()
                , held_securities_with_prices_repository = JSONHeldSecuritiesWithPricesRepository()
            )
        )
        kafka_consumer.consume()
    else:
        logging.error(f"Unconfigured data_type: {args.data_type}!")
        return 1



if __name__ == '__main__':
    main()
