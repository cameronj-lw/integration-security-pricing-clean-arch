
# core python
from dataclasses import dataclass
import datetime
import logging
from typing import List, Union

# native
from app.application.models import ColumnConfig, UserWithColumnConfig, PricingAttachment, DateWithPricingAttachments
from app.application.repositories import UserWithColumnConfigRepository, DateWithPricingAttachmentsRepository
from app.application.validators import (
    LWIDSchema, ColumnConfigPayloadSchema, FilePayloadSchema, PriceByIMEXSchema
)

from app.domain.event_publishers import EventPublisher
from app.domain.events import (
    SecurityCreatedEvent, PriceCreatedEvent,
    SecurityWithPricesCreatedEvent, PriceFeedCreatedEvent,
    PriceFeedWithStatusCreatedEvent, PriceAuditEntryCreatedEvent,
    PriceSourceCreatedEvent, PriceTypeCreatedEvent
)
from app.domain.models import (
    Security, Price, SecurityWithPrices, PriceFeed,
    PriceFeedWithStatus, PriceAuditEntry, PriceSource, PriceType
)
from app.domain.repositories import (
    SecurityRepository, PriceRepository, SecurityWithPricesRepository, PriceFeedRepository,
    PriceFeedWithStatusRepository, PriceAuditEntryRepository, PriceSourceRepository, PriceTypeRepository
)



def validate_payload(payload, schema):
    """ Generic function to validate payloads """
    errors = schema.validate(payload)
    if errors:
        raise InvalidPayloadException(errors)


class InvalidPayloadException(Exception):
    pass


@dataclass
class SecurityCommandHandler:
    repo: SecurityRepository

    def handle_post(self, payload):
        """ Handle a POST command.
        
        Args:
        - payload (list or dict): Payload to handle.

        Returns:
        - int: Number of items which were saved (e.g. row count, file saved count).
        """
        # Validate payload. If no exception is thrown here, it passed.
        try:
            validate_payload(payload, LWIDSchema())
        except InvalidPayloadException as e:
            # TODO_SUPPORT: logging? alerting?
            pass  # Exception should be caught by interface layer
        
        # Payload will be a dict, containing lw_id (List). Convert to Securities:
        logging.debug(f"Parsing {payload}")
        secs = [Security(lw_id) for lw_id in payload['lw_id']]
        
        # Save the security to the repository
        logging.debug(f"Creating {secs}")
        row_cnt = self.repo.create(secs)
        return row_cnt

    def handle_delete(self, payload):
        """ Handle a DELETE command.
        
        Args:
        - payload (list or dict): Payload to handle.

        Returns:
        - int: Number of items which were deleted (e.g. row count, file saved count).
        """
        # Validate payload. If no exception is thrown here, it passed.
        try:
            validate_payload(payload, ManualPricingSecuritySchema())
        except InvalidPayloadException as e:
            # TODO_SUPPORT: logging? alerting?
            pass  # Exception should be caught by interface layer
        
        # Payload will be a dict, containing lw_id (List). Convert to Securities:
        logging.debug(f"Parsing {payload}")
        secs = [Security(lw_id) for lw_id in payload['lw_id']]
        
        # Save the security to the repository
        logging.debug(f"Deleting {secs}")
        row_cnt = self.repo.delete(secs)
        return row_cnt


@dataclass
class UserWithColumnConfigCommandHandler:
    repo: UserWithColumnConfigRepository

    def handle_post(self, user_id, payload):
        """ Handle a POST command.
        
        Args:
        - payload (list or dict): Payload to handle.

        Returns:
        - int: Number of items which were saved (e.g. row count, file saved count).
        """
        # Validate payload. If no exception is thrown here, it passed.
        try:
            validate_payload(payload, ColumnConfigPayloadSchema())
        except InvalidPayloadException as e:
            # TODO_SUPPORT: logging? alerting?
            pass  # Exception should be caught by interface layer
        
        # Payload will be a dict, containing columns (List). Convert to Securities:
        logging.debug(f"Parsing {payload}")
        column_configs = [ColumnConfig(cc['column_name'], cc['is_hidden']) for cc in payload['columns']]
        user_with_column_config = UserWithColumnConfig(user_id, column_configs)
        
        # First, delete existing dataset for user_id
        logging.debug(f"Deleting for {user_id}")
        row_cnt = self.repo.delete(user_id)
        
        # Save the column config to the repository
        logging.debug(f"Creating {user_with_column_config}")
        row_cnt = self.repo.create(user_with_column_config)
        return row_cnt

    def handle_delete(self, payload):
        """ Handle a DELETE command.
        
        Args:
        - payload (list or dict): Payload to handle.

        Returns:
        - int: Number of items which were deleted (e.g. row count, file saved count).
        """
        # Validate payload. If no exception is thrown here, it passed.
        try:
            validate_payload(payload, ManualPricingSecuritySchema())
        except InvalidPayloadException as e:
            # TODO_SUPPORT: logging? alerting?
            pass  # Exception should be caught by interface layer
        
        # Payload will be a dict, containing lw_id (List). Convert to Securities:
        logging.debug(f"Parsing {payload}")
        secs = [Security(lw_id) for lw_id in payload['lw_id']]
        
        # Save the security to the repository
        logging.debug(f"Deleting {secs}")
        row_cnt = self.repo.delete(secs)
        return row_cnt


@dataclass
class PricingAttachmentByDateCommandHandler:
    repo: DateWithPricingAttachmentsRepository

    def handle_post(self, data_date, payload):
        """ Handle a POST command.
        
        Args:
        - payload (list or dict): Payload to handle.

        Returns:
        - int: Number of items which were saved (e.g. row count, file saved count).
        """
        try:
            date = datetime.datetime.strptime(data_date, '%Y%m%d').date()
        except Exception as e:
            # TODO: application error handling for invalid date?
            pass  # exception should be caught by interface layer
        
        # Validate payload. If no exception is thrown here, it passed.
        try:
            validate_payload(payload, FilePayloadSchema())
        except InvalidPayloadException as e:
            # TODO_SUPPORT: logging? alerting?
            pass  # Exception should be caught by interface layer
        
        # Payload will be a dict, containing files (List). Convert to PricingAttachments:
        logging.debug(f"Parsing {payload}")
        attachments = [PricingAttachment(name=f['name'], binary_content=f['binary_content']) for f in payload['files']]
        date_with_attachments = DateWithPricingAttachments(date, attachments)
        
        # Save the attachments to the repository
        logging.debug(f"Creating {date_with_attachments}")
        row_cnt = self.repo.create(date_with_attachments)
        return row_cnt


@dataclass
class PriceByIMEXCommandHandler:
    # We'll retrieve security attributes (including lw_id) from here
    sec_repo: SecurityRepository

    # We'll save prices to here
    price_repos: List[PriceRepository]

    def handle_post(self, payload):
        """ Handle a POST command.
        
        Args:
        - payload (list or dict): Payload to handle.

        Returns:
        - int: Number of items which were saved (e.g. row count, file saved count).
        """
        # Validate payload. If no exception is thrown here, it passed.
        try:
            validate_payload(payload, PriceByIMEXSchema())
        except InvalidPayloadException as e:
            # TODO_SUPPORT: logging? alerting?
            pass  # Exception should be caught by interface layer
        
        # Payload will be a list, containing dicts representing prices. Convert to Prices:
        logging.debug(f"Parsing {payload}")
        prices = []
        secs = self.sec_repo.get()
        for px_dict in payload['prices']:

            # Should come with a "from_date". We'll make this the data_date:
            px_dict['data_date'] = px_dict.pop('from_date')

            # Payload may not contain lw_id, therefore we need to find it based on pms_symbol
            pms_symbol = px_dict['apx_symbol']
            matching_secs = [s for s in secs if s.attributes['pms_symbol'] == pms_symbol]
            lw_id = matching_secs[0].lw_id
            px_dict['lw_id'] = lw_id

            # Requires modified_at - populate here:
            if 'modified_at' not in px_dict:
                px_dict['modified_at'] = datetime.datetime.now().isoformat()[:-3]

            # Append to master list of Prices
            prices.append(Price.from_dict(px_dict))
        
        # Save the price(s) to the repositor(ies)
        for price_repo in self.price_repos:
            logging.info(f"Creating {prices} in {price_repo}")
            row_cnt = price_repo.create(prices)
        
        return row_cnt

