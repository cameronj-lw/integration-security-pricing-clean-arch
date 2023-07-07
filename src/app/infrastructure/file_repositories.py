
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
from app.infrastructure.util.date import get_next_bday
from app.infrastructure.util.file import (
    prepare_dated_file_path, 
    get_read_model_content, set_read_model_content,
    get_read_model_file, get_read_model_folder
)


class DeleteFailedException(Exception):
    pass

class CreateFailedException(Exception):
    pass


class JSONHeldSecuritiesRepository(SecurityRepository):
    read_model_name = 'held_securities'
    file_name = 'lw_id.json'

    def create(self, data_date: datetime.date, securities: List[Security]) -> List[Security]:
        # Get list of lw_id's into JSON format
        held_lwids = [s.lw_id for s in securities]
        json_content = json.dumps(held_lwids, indent=4, default=str)
        
        # Save to file
        set_read_model_content(read_model_name=self.read_model_name, file_name=self.file_name, content=json_content, data_date=data_date)
        
        # Confirm it was successfully created. If not, throw exception.
        get_res = self.get(data_date)
        if get_res is None:
            raise CreateFailedException(f"Failed to create/update held securities list for {data_date.isoformat()}")
        else:
            return get_res

    def add_security(self, data_date: datetime.date, security: Security) -> Security:
        # Get existing list (excluding the provided lw_id), then append the security to it
        held_secs = self.get(data_date)
        if held_secs is None:
            held_secs = [security]
        else:
            held_secs = [s for s in held_secs if s.lw_id != security.lw_id]
            held_secs.append(security)
        
        # Now we have the new list of securities. Create.
        if self.create(data_date, held_secs) is not None:
            return security
        else:
            return None

    def delete(self, data_date: datetime.date, security: Security):
        # Get existing list, excluding the provided lw_id, into JSON format
        held_secs = self.get(data_date)
        held_lwids = [s.lw_id for s in held_secs if s.lw_id != security.lw_id]
        json_content = json.dumps(held_lwids, indent=4, default=str)

        # Save to file
        set_read_model_content(read_model_name=self.read_model_name, file_name=self.file_name, content=json_content, data_date=data_date)
        
        # Now the "get" should return None. Confirm this:
        if self.get(data_date=data_date, security=security) is not None:
            raise DeleteFailedException(f"Failed to remove {security.lw_id} from held securities list for {data_date.isoformat()}")

    def get(self, data_date: datetime.date, security: Union[Security, None] = None) -> List[Security]:
        if security is not None:
            # Retrieve only from the single file
            sec_dict = get_read_model_content(read_model_name='security', file_name=f'{security.lw_id}.json')
            if sec_dict is None:
                return None  # DNE yet
            else:
                sec = Security.from_dict(sec_dict)
                return [sec]
        else:
            # Retrieve all for the date
            res = []
            target_dir = get_read_model_folder(read_model_name='security')
            for dirpath, _, filenames in os.walk(target_dir):
                # Ignore subfolders
                if dirpath != target_dir:
                    continue
                
                for filename in filenames:
                    with open(filename, 'r') as f:
                        sec_dict = json.loads(f.read())
                        sec = Security.from_dict(sec_dict)
                    if sec is not None:
                        res.append(sec)
            return res


class JSONHeldSecuritiesWithPricesRepository(SecuritiesWithPricesRepository):
    read_model_name = 'held_securities_with_prices'
    file_name = 'held.json'

    def create(self, data_date: datetime.date, securities_with_prices: List[SecurityWithPrices]) -> List[SecurityWithPrices]:
        # Get secs with prices into JSON format
        securities_with_prices_dicts = [swp.to_dict() for swp in securities_with_prices]
        json_content = json.dumps(securities_with_prices_dicts, indent=4, default=str)
        
        # Save to file
        set_read_model_content(read_model_name=self.read_model_name, file_name=self.file_name, content=json_content, data_date=data_date)
        
        # Confirm it was successfully created. If not, throw exception.
        get_res = self.get(data_date)
        if get_res is None:
            raise CreateFailedException(f"Failed to create/update held securities with prices list for {data_date.isoformat()}")
        else:
            return get_res

    def refresh_for_securities(self, data_date: datetime.date, securities: List[Security]):
        # Find which securities to refresh. This will be the ones from the provided list which are held.
        held_secs = JSONHeldSecuritiesRepository().get(data_date)
        if held_secs is None:
            lw_ids_to_refresh = [s.lw_id for s in securities]
        else:
            held_lwids = [s.lw_id for s in held_secs]
            lw_ids_to_refresh = [s.lw_id for s in securities if s.lw_id in held_lwids]

        # Get other securities as a starting point
        orig_get_res = self.get(data_date)
        if orig_get_res is None:
            res = []
        else:
            res = [swp for swp in orig_get_res if swp.security.lw_id not in lw_ids_to_refresh]
        
        # Loop thru and append each security to result
        for lw_id in lw_ids_to_refresh:
            swp_dict = get_read_model_content(read_model_name=JSONHeldSecuritiesWithPricesRepository().read_model_name
                    , file_name=f'{lw_id}.json', data_date=data_date)
            swp = SecurityWithPrices.from_dict(swp_dict)
            res.append(swp)

        # Put into JSON format
        json_content = [swp.to_dict() for swp in res]

        # Save to file
        set_read_model_content(read_model_name=self.read_model_name, file_name=self.file_name, content=json_content, data_date=data_date)
        
        # Confirm it was successfully created. If not, throw exception.
        get_res = self.get(data_date)
        if get_res is None:
            raise CreateFailedException(f"Failed to refresh held securities with prices list with {len(securities)} securities for {data_date.isoformat()}")
        else:
            return get_res

    def get(self, data_date: datetime.date) -> List[SecurityWithPrices]:
        swps_dicts = get_read_model_content(read_model_name=self.read_model_name, file_name=self.file_name, data_date=data_date)
        if swps_dicts is None:
            return []
        securities_with_prices = [SecurityWithPrices.from_dict(d) for d in swps_dicts]
        return securities_with_prices


class JSONSecurityRepository(SecurityRepository):
    read_model_name = 'security'

    def create(self, security: Security):
        # Get dict
        sec_dict = security.to_dict()
        json_content = json.dumps(sec_dict, indent=4, default=str)
        
        # Save to file
        set_read_model_content(read_model_name=self.read_model_name, file_name=self.file_name, content=json_content, data_date=data_date)
        
        # Confirm it was successfully created. If not, throw exception.
        get_res = self.get(security)
        if get_res is None:
            raise CreateFailedException(f"Failed to add {security.lw_id} to JSON security repo")
        else:
            return get_res[0]

    def get(self, security: Union[Security, None] = None) -> List[Security]:
        if security is not None:
            # Retrieve only from the single file
            sec_dict = get_read_model_content(read_model_name=self.read_model_name, file_name=f'{security.lw_id}.json')
            if sec_dict is None:
                return None  # DNE yet
            else:
                sec = Security.from_dict(sec_dict)
                return [sec]
        else:
            # Retrieve all for the date
            res = []
            target_dir = get_read_model_folder(read_model_name=self.read_model_name)
            for dirpath, _, filenames in os.walk(target_dir):
                # Ignore subfolders
                if dirpath != target_dir:
                    continue
                
                for filename in filenames:
                    with open(filename, 'r') as f:
                        sec_dict = json.loads(f.read())
                        sec = Security.from_dict(sec_dict)
                    if sec is not None:
                        res.append(sec)
            return res


class JSONSecurityWithPricesRepository(SecurityWithPricesRepository):
    read_model_name = 'security_with_prices'

    def create(self, swp: SecurityWithPrices) -> SecurityWithPrices:
        swp_dict = swp.to_dict()
        json_content = json.dumps(swp_dict, indent=4, default=str)
        target_file = get_read_model_file(read_model_name=self.read_model_name, file_name=f'{swp.security.lw_id}.json', data_date=swp.data_date)
        with open(target_file, 'w') as f:
            logging.debug(f'writing to {target_file}:\n{json_content}')
            f.write(json_content)
        # Confirm it was successfully created. If not, throw exception.
        get_res = self.get(swp.data_date, swp.security)
        if get_res is None:
            raise CreateFailedException(f"Failed to add {swp.security.lw_id} to security with prices repo for {swp.data_date.isoformat()}")
        else:
            return get_res[0]

    def add_price(self, price: Price, mode='curr') -> SecurityWithPrices:
        if mode == 'prev':
            data_date = get_next_bday(price.data_date)
        else:
            data_date = price.data_date
        swp = self.get(data_date=data_date, security=price.security)[0]
        if swp is None:
            # Just need to create the file with security info, plus this new price:
            return self.create(SecurityWithPrices(security=price.security, data_date=data_date, prices=[price]))
        else:
            # Need to replace the existing price from the provided source & type, 
            # or add this price if there's not yet a price with the provided source & type
            prices = [px for px in swp.prices if (px.source != price.source or px._type != price._type)]
            prices.append(price)
            swp.prices = prices 
            return self.create(swp)

    def add_security(self, data_date: datetime.date, security: Security) -> SecurityWithPrices:
        swps = self.get(data_date=data_date, security=security)
        if swps is None:
            # Just need to create the file with security info, plus this new price:
            return self.create(SecurityWithPrices(security=security, data_date=data_date, prices=[]))
        else:
            # Need to replace the existing security
            swp = swps[0]
            swp.security = security
            return self.create(swp)

    def get(self, data_date: datetime.date, security: Union[Security, None] = None) -> List[SecurityWithPrices]:
        if security is not None:
            # Retrieve only from the single file
            swp_dict = get_read_model_content(read_model_name=self.read_model_name, file_name=f'{security.lw_id}.json', data_date=data_date)
            if swp_dict is None:
                return None  # DNE yet
            else:
                swp = SecurityWithPrices.from_dict(swp_dict)
                return [swp]
        else:
            # Retrieve all for the date
            res = []
            target_dir = get_read_model_folder(read_model_name=self.read_model_name, data_date=data_date)
            for dirpath, _, filenames in os.walk(target_dir):
                # Ignore subfolders
                if dirpath != target_dir:
                    continue
                
                for filename in filenames:
                    with open(filename, 'r') as f:
                        swp_dict = json.loads(f.read())
                        swp = SecurityWithPrices.from_dict(swp_dict)
                    if swp is not None:
                        res.append(swp)
            return res


class JSONPriceAuditEntryRepository(PriceAuditEntryRepository):
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

