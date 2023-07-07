
# core python
from dataclasses import dataclass
import datetime
import logging
from typing import List, Union

# native
from app.application.models import ColumnConfig, UserWithColumnConfig, PricingAttachment, DateWithPricingAttachments
from app.application.repositories import UserWithColumnConfigRepository, DateWithPricingAttachmentsRepository
from app.application.validators import (
    ManualPricingSecuritySchema, ColumnConfigPayloadSchema, FilePayloadSchema
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
            validate_payload(payload, ManualPricingSecuritySchema())
        except InvalidPayloadException as e:
            # TODO_SUPPORT: logging? alerting?
            pass  # Exception should be caught by interface layer
        
        # Payload will be a dict, containing lw_id (List). Convert to Securities:
        logging.info(f"Parsing {payload}")
        secs = [Security(lw_id) for lw_id in payload['lw_id']]
        
        # Save the security to the repository
        logging.info(f"Creating {secs}")
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
        logging.info(f"Parsing {payload}")
        secs = [Security(lw_id) for lw_id in payload['lw_id']]
        
        # Save the security to the repository
        logging.info(f"Deleting {secs}")
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
        logging.info(f"Parsing {payload}")
        column_configs = [ColumnConfig(cc['column_name'], cc['is_hidden']) for cc in payload['columns']]
        user_with_column_config = UserWithColumnConfig(user_id, column_configs)
        
        # First, delete existing dataset for user_id
        logging.info(f"Deleting for {user_id}")
        row_cnt = self.repo.delete(user_id)
        
        # Save the column config to the repository
        logging.info(f"Creating {user_with_column_config}")
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
        logging.info(f"Parsing {payload}")
        secs = [Security(lw_id) for lw_id in payload['lw_id']]
        
        # Save the security to the repository
        logging.info(f"Deleting {secs}")
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
        logging.info(f"Parsing {payload}")
        attachments = [PricingAttachment(name=f['name'], binary_content=f['binary_content']) for f in payload['files']]
        date_with_attachments = DateWithPricingAttachments(date, attachments)
        
        # Save the attachments to the repository
        logging.info(f"Creating {date_with_attachments}")
        row_cnt = self.repo.create(date_with_attachments)
        return row_cnt
        
