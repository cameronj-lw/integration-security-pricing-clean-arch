
# core python
from abc import abstractmethod
from dataclasses import dataclass
import datetime
import json
import logging
import time
from typing import List, Type, Union


# pypi
from confluent_kafka import Consumer, OFFSET_BEGINNING, OFFSET_END

# native
from app.domain.events import (
    Event, AppraisalBatchCreatedEvent, PriceBatchCreatedEvent, SecurityCreatedEvent,
    PositionCreatedEvent, PositionDeletedEvent, PortfolioCreatedEvent
)
from app.domain.event_handlers import EventHandler
from app.domain.event_subscribers import EventSubscriber
from app.domain.exceptions import (
    SecurityNotFoundException, PortfolioNotFoundException
)
from app.domain.message_brokers import MessageBroker
from app.domain.models import (
    Security, AppraisalBatch, PriceBatch, PriceSource
    , Position, Portfolio
)
from app.domain.repositories import PortfolioRepository

from app.infrastructure.message_brokers import KafkaBroker
from app.infrastructure.sql_repositories import (
    CoreDBPortfolioRepository, CoreDBSecurityRepository
)
from app.infrastructure.util.config import AppConfig



class DeserializationError(Exception):
    pass


class KafkaEventConsumer(EventSubscriber):
    def __init__(self, topics, event_handler):
        super().__init__(message_broker=KafkaBroker(), topics=topics, event_handler=event_handler)
        self.config = dict(self.message_broker.config)
        self.config.update(AppConfig().parser['kafka_consumer'])
        print(f'Created KafkaEventConsumer with config: {type(self.config)} {self.config}')
        self.consumer = Consumer(self.config)
        # self.consumer.subscribe(self.topics, on_assign=self.on_assign)
        # TODO: remove above when not needed
    
    # TODO: delete once not needed, i.e. once confirmed using dataclass with post_init is desired
    # def __init__(self, config_file: str, reset_offset: bool, topics: List[str], event_class: Type[Event]):
    #     # super().__init__()  # TODO: remove if not needed, i.e. if not using EventHandler as base class
    #     self.config_file = config_file
    #     self.reset_offset = reset_offset
    #     self.topics = topics
    #     # Create the Consumer based on config file
    #     self.config_parser = ConfigParser()
    #     self.config_parser.read_file(self.config_file)
    #     self.config = dict(self.config_parser['default'])
    #     self.config.update(self.config_parser['consumer'])
    #     self.consumer.subscribe(self.topics, on_assign=reset_offset)
    #     try:  # TODO: should this not belong inside init method?
    #         while True:
    #             msg = self.consumer.poll(1.0)
    #     except KeyboardInterrupt:
    #         pass
    #     finally:
    #         # Leave group and commit final offsets
    #         self.consumer.close()

    def consume(self, reset_offset: bool=False, async_commit=False):
        self.reset_offset = reset_offset
        self.consumer.subscribe(self.topics, on_assign=self.on_assign)
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    logging.debug("Waiting...")
                elif msg.error():
                    logging.error("ERROR: %s".format(msg.error()))
                    # TODO: raise exception?
                elif msg.value() is not None:
                    logging.info(f"Consuming message: {msg.value()}")
                    should_commit = True  # commit at the end, unless this gets overridden below
                    try:
                        event = self.deserialize(msg.value())

                        if event is None:
                            # A deserialize method returning None means the kafka message
                            # does not meet criteria for representing an Event that needs handling.
                            # Therefore if reaching here we should simply commit offset.
                            self.consumer.commit(message=msg, asynchronous=async_commit)
                            continue
                        
                        # If reaching here, we have an Event that should be handled:
                        logging.info(f"Handling {event}")
                        should_commit = self.event_handler.handle(event)
                        logging.info(f"Done handling {event}")
                    
                    except Exception as e:
                        if isinstance(e, DeserializationError):
                            logging.exception(f'Exception while deserializing: {e}')
                            should_commit = self.event_handler.handle_deserialization_error(e)
                        else:
                            logging.exception(e)  # TODO: any more valuable logging?
                    
                    # Commit, unless we should not based on above results
                    if should_commit:
                        self.consumer.commit(message=msg, asynchronous=async_commit)
                        logging.info("Done committing offset")
                    else:
                        logging.info("Not committing offset, likely due to the most recent exception")
                # time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self.consumer.close()


    def on_assign(self, consumer, partitions):
        # TODO: confirm this works as class method
        if self.reset_offset:
            for p in partitions:
                logging.info(f"Resetting offset for {p}")
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # TODO: remove when not needed
    # def get_single_event(self) -> self.event_class:
    #     while True:
    #         msg = self.consumer.poll(1.0)
    #         if msg is None:
    #             # Initial message consumption may take up to
    #             # `session.timeout.ms` for the consumer group to
    #             # rebalance and start consuming
    #             logging.debug("Waiting...")
    #         elif msg.error():
    #             logging.error("ERROR: %s".format(msg.error()))
    #         elif msg.value() is not None:
    #             # Extract the (optional) key and value, transform, and produce to coredb topic.
    #             return self.deserialize(msg.value())

    @abstractmethod
    def deserialize(self, message_value: bytes) -> Union[Event, None]:  # TODO: does the message always have to be bytes?
        """ 
        Subclasses of KafkaEventConsumer must implement a deserialize method.
        Returning None (rather than an Event) signifies that there is no Event to handle.
        This makes sense when the consumer is looking for specific criteria to represent 
        the desired Event, but that criteria is not necessarily met in every message from the topic(s).
        """
        
    def __del__(self):
        self.consumer.close()


class KafkaCoreDBSecurityCreatedEventConsumer(KafkaEventConsumer):
    def __init__(self, event_handler: EventHandler):
        """ Creates a KafkaEventConsumer to consume new/changed coredb securities with the provided event handler """
        super().__init__(event_handler=event_handler, topics=[AppConfig().get('kafka_topics', 'coredb_security')])

    def deserialize(self, message_value: bytes) -> SecurityCreatedEvent:
        event_dict = json.loads(message_value.decode('utf-8'))
        lw_id = event_dict['lw_id']
        attributes = {k:event_dict[k] for k in event_dict if k != 'lw_id'}

        # Message could contain a modified_at, in seconds since epoch
        if 'modified_at' in attributes:
            if isinstance(attributes['modified_at'], int):
                attributes['modified_at'] = datetime.datetime.fromtimestamp(attributes['modified_at'] / 1000.0)
            attributes['modified_at'] = attributes['modified_at'].isoformat()

        logging.debug(f'KafkaCoreDBSecurityCreatedEventConsumer lw_id: {lw_id} type {type(lw_id)}')
        sec = Security(lw_id=lw_id, attributes=attributes)
        logging.debug(f'KafkaCoreDBSecurityCreatedEventConsumer sec: {sec} type {type(sec)}')
        event = SecurityCreatedEvent(sec)
        logging.debug(f'KafkaCoreDBSecurityCreatedEventConsumer event: {event} type {type(event)}')
        return event


class KafkaCoreDBAppraisalBatchCreatedEventConsumer(KafkaEventConsumer):
    def __init__(self, event_handler: EventHandler):
        """ Creates a KafkaEventConsumer to consume new/changed coredb appraisal batches with the provided event handler """
        super().__init__(event_handler=event_handler, topics=[AppConfig().get('kafka_topics', 'coredb_appraisal_batch')])

    def deserialize(self, message_value: bytes) -> AppraisalBatchCreatedEvent:
        # Get dict from Kafka message
        event_dict = json.loads(message_value.decode('utf-8'))
        event_dict = {k.lower(): v for k, v in event_dict.items()}
        
        # Populate default for portfolios ... note this should not be long-term
        if 'portfolios' not in event_dict:
            event_dict['portfolios'] = '@LW_OpenandMeasurementandTest'  # TODO: should this be an assumed default?
        
        # Convert "days since epoch" to date
        date = (datetime.datetime(year=1970, month=1, day=1) + datetime.timedelta(days=event_dict['data_date'])).date()

        # Create batch, then event and return it
        batch = AppraisalBatch(portfolios=event_dict['portfolios'], data_date=date)
        event = AppraisalBatchCreatedEvent(batch)
        return event


class KafkaCoreDBPriceBatchCreatedEventConsumer(KafkaEventConsumer):
    def __init__(self, event_handler: EventHandler):
        """ Creates a KafkaEventConsumer to consume new/changed coredb price batches with the provided event handler """
        super().__init__(event_handler=event_handler, topics=[AppConfig().get('kafka_topics', 'coredb_price_batch')])

    def deserialize(self, message_value: bytes) -> PriceBatchCreatedEvent:
        event_dict = json.loads(message_value.decode('utf-8'))
        event_dict = {k.lower(): v for k, v in event_dict.items()}
        date = (datetime.datetime(year=1970, month=1, day=1) + datetime.timedelta(days=event_dict['data_date'])).date()
        batch = PriceBatch(source=PriceSource(event_dict['source']), data_date=date)
        event = PriceBatchCreatedEvent(batch)
        return event


class KafkaAPXPositionEventConsumer(KafkaEventConsumer):
    def __init__(self, event_handler: EventHandler):
        """ Creates a KafkaEventConsumer to consume new/deleted apxdb positions with the provided event handler """
        super().__init__(event_handler=event_handler, topics=[AppConfig().get('kafka_topics', 'apxdb_position')])

    def deserialize(self, message_value: bytes) -> Union[PositionCreatedEvent, PositionDeletedEvent]:
        event_dict = json.loads(message_value.decode('utf-8'))

        # Get vals. For a d (delete) this will be in payload->before, else payload->after.
        if event_dict['payload']['op'] in ('d'):
            vals = {k.lower():v for k, v in event_dict['payload']['before'].items()}
        else:
            vals = {k.lower():v for k, v in event_dict['payload']['after'].items()}

        # Create portfolio, note we should only need the pms_portfolio_id
        portfolio = Portfolio(portfolio_code='', attributes={'pms_portfolio_id': vals['portfolioid']})

        # Create security, note we should only need the pms_security_id
        security = Security(lw_id='', attributes={'pms_security_id': vals['securityid']})

        # Include the pms_position_id in attributes, if it exists
        position_attributes = vals.copy()
        if 'positionid' in vals and 'pms_position_id' not in vals:
            position_attributes.update({'pms_position_id':vals['positionid']})

        # Create Position instance
        position = Position(portfolio=portfolio, data_date=datetime.date.today()
            , security=security, quantity=vals['quantity'], is_short=vals['isshortposition']
            , attributes=position_attributes)

        # Create and return event, either a delete or create
        if event_dict['payload']['op'] in ('c', 'u', 'r'):
            return PositionCreatedEvent(position)
        elif event_dict['payload']['op'] in ('d'):
            return PositionDeletedEvent(position)
        else:
            # TODO: unrecognized operation exception?
            return None 


class KafkaAPXPortfolioEventConsumer(KafkaEventConsumer):
    def __init__(self, event_handler: EventHandler, portfolio_repository: PortfolioRepository):
        """ Creates a KafkaEventConsumer to consume new/deleted apxdb portfolios with the provided event handler """
        super().__init__(event_handler=event_handler, topics=[AppConfig().get('kafka_topics', 'apxdb_portfolio')
                , AppConfig().get('kafka_topics', 'apxdb_aoobject')])
        
        # We'll retrieve portfolios from here, to differentiate messages 
        # reresenting portfolios vs. not portfolios
        self.portfolio_repository = portfolio_repository

    def deserialize(self, message_value: bytes) -> Union[PortfolioCreatedEvent, None]:
        event_dict = json.loads(message_value.decode('utf-8'))

        # Get vals. For a d (delete) this will be in payload->before, else payload->after.
        if event_dict['payload']['op'] in ('d'):
            return None  # Delete operation, return None since we don't care about a delete
        else:
            vals = {k.lower():v for k, v in event_dict['payload']['after'].items()}

        # Get the portfolio. Will also be used to determine whether this message is regarding a Portfolio.
        portfolio_id = vals['portfolioid'] if 'portfolioid' in vals else vals['objectid']
        portfolios = self.portfolio_repository.get(portfolio_id=portfolio_id)
        if not len(portfolios):
            logging.info(f"Ignoring message since there was no portfolio with PortfolioID {portfolio_id}")
            return None
        portfolio = portfolios[0]

        # If we made it here, we have found the portfolio corresponding to the message. 
        # Create the Event and return it.
        return PortfolioCreatedEvent(portfolio)



