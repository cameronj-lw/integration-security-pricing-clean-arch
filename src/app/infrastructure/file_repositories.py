
# core python
import datetime
import json
import logging
import os
from typing import List, Union

# native
from app.application.models import PricingAttachment, DateWithPricingAttachments
from app.application.repositories import DateWithPricingAttachmentsRepository

from app.domain.models import (
    Price, Security, PriceAuditEntry, SecurityWithPrices
)
from app.domain.repositories import (
    SecurityRepository, SecurityWithPricesRepository
    , SecuritiesWithPricesRepository
    , PriceAuditEntryRepository
)

from app.infrastructure.util.config import AppConfig
from app.infrastructure.util.file import prepare_dated_file_path, get_read_model_content, get_read_model_file


class JSONReadModelHeldSecurityRepository(SecurityRepository):
    def create(self, security: Security) -> Security:
        pass  # TODO: implement

    def get(self, lw_id: str) -> List[Security]:
        pass


class JSONReadModelHeldSecuritiesWithPricesRepository(SecuritiesWithPricesRepository):
    def create(self, security_with_prices: SecurityWithPrices) -> SecurityWithPrices:
        pass

    def get(self, data_date: datetime.date) -> List[SecurityWithPrices]:
        pass


class JSONReadModelSecurityRepository(SecurityRepository):
    def create(self, security: Security):
        data_dir = AppConfig().parser.get('files', 'data_dir')
        base_dir = os.path.join(data_dir, 'lw', 'pricing_read_models', 'security')
        target_file = prepare_dated_file_path(folder_name=base_dir, date=data_date
                , file_name=f'{Security.lw_id}.json', rotate=False)
        dict_content = Security.to_dict()
        json_content = json.dumps(dict_content, indent=4, default=str)
        with open(target_file, 'w') as f:
            logging.debug(f'writing to {target_file}:\n{json_content}...')
            f.write(json_content)

    def add_price(self, price: Price, mode='curr') -> SecurityWithPrices:
        pass

    def get(self, data_date: datetime.date, security: Union[Security, None] = None) -> List[Security]:
        pass


class JSONReadModelSecurityWithPricesRepository(SecurityWithPricesRepository):
    def create(self, security_with_prices: SecurityWithPrices) -> SecurityWithPrices:
        pass

    def add_price(self, price: Price, mode='curr') -> SecurityWithPrices:
        pass

    def add_or_update_security(self, data_date: datetime.date, security: Security) -> SecurityWithPrices:
        existing_dict = get_read_model_content(read_model_name='security_with_prices', file_name=f'{Security.lw_id}.json', data_date=data_date)
        target_file = get_read_model_file(read_model_name='security_with_prices', file_name=f'{Security.lw_id}.json', data_date=data_date)
        if existing_dict is None:
            # This means there are no prices. Just need to create the file with security info:
            json_content = security.to_dict()
            with open(target_file, 'w') as f:
                logging.debug(f'writing to {target_file}:\n{json_content}')
                f.write(json_content)
            return
        else:
            # Get keys & values which exist in the current file, but are not part of the security info:
            existing_dict = get_read_model_content(read_model_name='security_with_prices', file_name=f'{Security.lw_id}.json', data_date=data_date)
            sec_dict = security.to_dict()
            new_dict = existing_dict.copy()  # {k:existing_dict[k] for k in existing_dict if k not in sec_dict}
            new_dict.update(sec_dict)
            json_content = new_dict
            with open(target_file, 'w') as f:
                logging.debug(f'writing to {target_file}:\n{json_content}')
                f.write(json_content)

    def get(self, data_date: datetime.date, security: Security) -> List[SecurityWithPrices]:
        pass


class JSONReadModelPriceAuditEntryRepository(PriceAuditEntryRepository):
    def create(self, price_audit_entry: PriceAuditEntry) -> PriceAuditEntry:
        pass

    def get(self, data_date: datetime.date, security: Security) -> List[PriceAuditEntry]:
        pass


class DataDirDateWithPricingAttachmentsRepository(DateWithPricingAttachmentsRepository):
    def create(self, date_with_attachments: DateWithPricingAttachments) -> int:
        data_dir = AppConfig().parser.get('files', 'data_dir')
        base_dir = os.path.join(data_dir, 'lw', 'pricing_audit')
        target_dir = prepare_dated_file_path(folder_name=base_dir, date=date_with_attachments.data_date, file_name='', rotate=False)
        for f in date_with_attachments.attachments:
            # Create the file path
            file_path = os.path.join(target_dir, f.name)
            # Save the binary content to the file
            with open(file_path, "wb") as fp:
                logging.warning(f)
                fp.write(f.binary_content.encode('utf-8'))
        return len(date_with_attachments.attachments)

    def get(self, data_date: datetime.date) -> DateWithPricingAttachments:
        data_dir = AppConfig().parser.get('files', 'data_dir')
        base_dir = os.path.join(data_dir, 'lw', 'pricing_audit')
        target_dir = prepare_dated_file_path(folder_name=base_dir, date=data_date, file_name='', rotate=False)
        attachments = []
        for f in os.listdir(target_dir):
            attachment = PricingAttachment(name=f, full_path=os.path.join(target_dir, f))
            attachments.append(attachment)
        return DateWithPricingAttachments(data_date, attachments)

