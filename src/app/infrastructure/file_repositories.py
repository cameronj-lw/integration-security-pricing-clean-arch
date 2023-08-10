
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
    , SecuritiesForDateRepository
)

from app.infrastructure.sql_repositories import (
    CoreDBHeldSecurityRepository, CoreDBSecurityWithPricesRepository
    , APXDBHeldSecurityRepository
)
from app.infrastructure.util.config import AppConfig
from app.infrastructure.util.date import get_previous_bday, get_current_bday, get_next_bday
from app.infrastructure.util.file import (
    prepare_dated_file_path, 
    get_read_model_content, set_read_model_content,
    get_read_model_file, get_read_model_folder
)


class DeleteFailedException(Exception):
    pass

class CreateFailedException(Exception):
    pass


class JSONHeldSecuritiesRepository(SecuritiesForDateRepository):
    read_model_name = 'held_securities'
    file_name = 'lw_id.json'

    def create(self, data_date: datetime.date, securities: List[Security]) -> List[Security]:
        # Get list of lw_id's
        held_lwids = [s.lw_id for s in securities]
        
        # Save to file
        set_read_model_content(read_model_name=self.read_model_name, file_name=self.file_name, content=held_lwids, data_date=data_date)
        
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
        lw_ids = get_read_model_content(read_model_name=self.read_model_name
                , file_name=self.file_name, data_date=data_date)
        return None if lw_ids is None else [Security(lw_id) for lw_id in lw_ids]


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

    def refresh_for_securities(self, data_date: datetime.date, securities: List[Security], remove_other_secs=False):

        # Find which securities to refresh. This will be the ones from the provided list which are held.
        held_secs =  CoreDBHeldSecurityRepository().get(data_date=data_date)  # CoreDBHeldSecurityRepository().get(data_date)
        if held_secs is None:
            logging.info(f'No held_secs found')
            lw_ids_to_refresh = [s.lw_id for s in securities]
        else:
            held_lwids = [s.lw_id for s in held_secs]
            logging.info(f'{len(held_lwids)} held_secs found')
            lw_ids_to_refresh = [s.lw_id for s in securities if s.lw_id in held_lwids]

        # Get other securities as a starting point
        orig_get_res = self.get(data_date)
        if orig_get_res is None or remove_other_secs:
            res = []
        else:
            res = [swp for swp in orig_get_res if swp.security.lw_id not in lw_ids_to_refresh]
        
        # Loop thru and append each security to result
        logging.info(f'Refreshing master RM for {len(lw_ids_to_refresh)} securities...')
        logged = False

        # Get SWPs - query once to avoid many queries to DB for each security
        securities_with_prices = CoreDBSecurityWithPricesRepository().get(data_date=data_date, security=securities)

        for lw_id in lw_ids_to_refresh:
            sec_swps = [swp for swp in securities_with_prices if swp.security.lw_id == lw_id]
            sec_swp = sec_swps[0] if len(sec_swps) else None
            if sec_swp is not None:
                if not logged:
                    logging.debug(f'Appending {sec_swp}')
                    logged = True
                res.append(sec_swp)

        # TODO_CLEANUP: remove once not needed, i.e. when deciding to retire usage of the security_with_prices RM
        # # Loop thru and append each security to result
        # logging.info(f'Refreshing master RM for {len(lw_ids_to_refresh)} securities...')
        # logged = False
        # for lw_id in lw_ids_to_refresh:
        #     logging.info(f'Getting read model {JSONSecurityWithPricesRepository().read_model_name} for {data_date}')
        #     swp_dict = get_read_model_content(read_model_name=JSONSecurityWithPricesRepository().read_model_name
        #             , file_name=f'{lw_id}.json', data_date=data_date)
        #     logging.info(f'Refreshing for {lw_id}: {swp_dict}')
        #     swp = SecurityWithPrices.from_dict(swp_dict)
        #     if swp is not None:
        #         if not logged:
        #             logging.debug(f'Appending {swp}')
        #             logged = True
        #         res.append(swp)

        # Put into JSON format
        swp_dicts = [swp.to_dict() for swp in res]

        # Save to file
        set_read_model_content(read_model_name=self.read_model_name, file_name=self.file_name, content=swp_dicts, data_date=data_date)
        
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


# TODO_CLEANUP: remove when not needed
# class JSONSecurityRepository(SecurityRepository):
#     read_model_name = 'security'

#     def create(self, security: Security):
#         # Get dict
#         sec_dict = security.to_dict()
#         json_content = json.dumps(sec_dict, indent=4, default=str)
        
#         # Save to file
#         set_read_model_content(read_model_name=self.read_model_name, file_name=self.file_name, content=json_content, data_date=data_date)
        
#         # Confirm it was successfully created. If not, throw exception.
#         get_res = self.get(security)
#         if get_res is None:
#             raise CreateFailedException(f"Failed to add {security.lw_id} to JSON security repo")
#         else:
#             return get_res[0]

#     def get(self, security: Union[Security, None] = None) -> List[Security]:
#         if security is not None:
#             # Retrieve only from the single file
#             sec_dict = get_read_model_content(read_model_name=self.read_model_name, file_name=f'{security.lw_id}.json')
#             if sec_dict is None:
#                 return None  # DNE yet
#             else:
#                 sec = Security.from_dict(sec_dict)
#                 return [sec]
#         else:
#             # Retrieve all for the date
#             res = []
#             target_dir = get_read_model_folder(read_model_name=self.read_model_name)
#             for dirpath, _, filenames in os.walk(target_dir):
#                 # Ignore subfolders
#                 if dirpath != target_dir:
#                     continue
                
#                 for filename in filenames:
#                     with open(filename, 'r') as f:
#                         sec_dict = json.loads(f.read())
#                         sec = Security.from_dict(sec_dict)
#                     if sec is not None:
#                         res.append(sec)
#             return res


class JSONSecurityWithPricesRepository(SecurityWithPricesRepository):
    read_model_name = 'security_with_prices'

    def create(self, swp: SecurityWithPrices) -> SecurityWithPrices:
        # Get into JSON format
        swp_dict = swp.to_dict()  # self.get(swp.data_date, swp.security)[0].to_dict()  # get_supplemented_dict(swp)
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

    # TODO_CLEANUP: remove when not needed
    # def get_supplemented_dict(self, swp: SecurityWithPrices):
    #     swp_dict = swp.to_dict()

    #     # Get curr & prev bday:
    #     curr_bday, prev_bday = get_current_bday(swp.data_date), get_previous_bday(swp.data_date)

    #     # Add curr bday price(s)
    #     curr_bday_prices = [px for px in swp.prices if px.data_date == curr_bday]
    #     swp_dict['curr_bday_prices'] = [px.to_dict() for px in curr_bday_prices]
        
    #     # Add prev bday price(s)
    #     prev_bday_prices = [px for px in swp.prices if px.data_date == prev_bday]
    #     if len(prev_bday_prices):
    #         swp_dict['prev_bday_price'] = prev_bday_prices[0].to_dict()
    #     else:
    #         swp_dict['prev_bday_price'] = {}

    #     # Determine and add chosen price
    #     chosen_price = None
    #     for px in curr_bday_prices:
    #         if chosen_price is None:
    #             chosen_price = px
    #         elif px.source > chosen_price.source:  # See __gt__ method in domain model
    #             chosen_price = px
    #     swp_dict['chosen_price'] = {} if chosen_price is None else chosen_price.to_dict()
    #     return swp_dict

    def add_price(self, price: Price, mode='curr') -> SecurityWithPrices:
        logging.debug(f'Adding price: {price}')
        if mode == 'prev':
            data_date = get_next_bday(price.data_date)
        else:
            data_date = price.data_date
        swp = self.get(data_date=data_date, security=price.security)
        logging.debug(f'Found swps: {swp}')
        if swp is None:
            # Just need to create the file with security info, plus this new price:
            logging.debug(f'No swp found. Creating...')
            if mode == 'prev':
                return self.create(SecurityWithPrices(security=price.security, data_date=data_date, prev_bday_price=price))
            else:
                return self.create(SecurityWithPrices(security=price.security, data_date=data_date, curr_bday_prices=[price]))
        elif swp[0] is None:
            # Just need to create the file with security info, plus this new price:
            logging.debug(f'swp[0] is None. Creating...')
            if mode == 'prev':
                return self.create(SecurityWithPrices(security=price.security, data_date=data_date, prev_bday_price=price))
            else:
                return self.create(SecurityWithPrices(security=price.security, data_date=data_date, curr_bday_prices=[price]))
        else:
            swp = swp[0]
            logging.debug(f'Found swp: {swp}')

            # If adding prev bday price, we can just replace the existing one (if it already has one):
            if mode == 'prev':
                swp.prev_bday_price = price
            # Otherwise, we need to add the price to any others from curr day (from other sources):
            else:
                curr_bday_prices = [px for px in swp.curr_bday_prices if (px.source != price.source)]
                curr_bday_prices.append(price)
                swp.curr_bday_prices = curr_bday_prices 

            # Finally, create the SWP and return it
            logging.debug(f'Creating SecurityWithPrice... {swp}')
            return self.create(swp)

    def add_security(self, data_date: datetime.date, security: Security) -> SecurityWithPrices:
        swps = self.get(data_date=data_date, security=security)
        if swps is None:
            # Just need to create the file with security info:
            return self.create(SecurityWithPrices(security=security, data_date=data_date))
        else:
            # Need to replace the existing security
            logging.debug(f'swps: {swps}')
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
                logging.debug(f'Creating SWP from dict: {swp_dict}')
                swp = SecurityWithPrices.from_dict(swp_dict)
                logging.debug(f'Created {swp}')
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
        base_dir = os.path.join(data_dir, 'lw', 'security_pricing', 'audit')
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
        base_dir = os.path.join(data_dir, 'lw', 'security_pricing', 'audit')
        target_dir = prepare_dated_file_path(folder_name=base_dir, date=data_date, file_name='', rotate=False)
        attachments = []
        for f in os.listdir(target_dir):
            attachment = PricingAttachment(name=f, full_path=os.path.join(target_dir, f))
            attachments.append(attachment)
        return DateWithPricingAttachments(data_date, attachments)

