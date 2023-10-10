
# core python
import datetime
import json
import logging
import os
import re
from typing import List, Union, Tuple

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
    , APXDBHeldSecurityRepository, APXDBIMEXLogFolderRepository
    , CoreDBSecurityRepository
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
    # TODO_CLEANUP: retire this class if not being used?

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
        # Get secs with prices into dict format
        swps_dicts = [swp.to_dict() for swp in securities_with_prices]
        
        # Save to file
        set_read_model_content(read_model_name=self.read_model_name, file_name=self.file_name, content=swps_dicts, data_date=data_date)
        
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

        # Put into JSON format
        swp_dicts = []
        for swp in res:
            swp_dict = swp.to_dict()
            # Need to replace audit_trail which are empty arrays with None, per Verve #5146
            # TODO: could the front-end be changed to work with an empty array rather than requiring null if empty?
            # If so, this whole section can & should be condensed to use list comprehension as follows:
            # swp_dicts = [swp.to_dict() for swp in res]
            if 'audit_trail' in swp_dict:
                if isinstance(swp_dict['audit_trail'], list):
                    if not len(swp_dict['audit_trail']):
                        swp_dict['audit_trail'] = None
            # Now can append to the master list of dicts
            swp_dicts.append(swp_dict)        

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

    def remove_securities(self, data_date: datetime.date, securities: Union[List[Security], Security]):
        if isinstance(securities, Security):
            # Need to convert to List
            securities = [securities]
        
        # Get which lw_ids we want to remove (assumption is because they are no longer held)
        # Some securities may have an empty lw_id. For those we can find the lw_id based on pms_security_id instead
        lw_ids_to_remove = []
        for sec in securities:
            if len(sec.lw_id):
                # If we have an lw_id, add it
                lw_ids_to_remove.append(sec.lw_id)
            else:
                # lw_id may be blank, in which case we need to find it.
                # Assumption: if there is not lw_id, there must be a pms_security_id
                secs = CoreDBSecurityRepository().get(pms_security_id=sec.attributes['pms_security_id'])
                logging.info(f"Found {len(secs)} securities with pms_security_id {sec.attributes['pms_security_id']}")
                if len(secs):
                    lw_ids_to_remove.append(secs[0].lw_id)

        # Get existing SWPs, then remove items based on lw_id
        existing_swps = self.get(data_date)
        new_swps = [swp for swp in existing_swps if swp.security.lw_id not in lw_ids_to_remove]

        # Now we have all SWPs which should still be in the repo. Create method should fully replace the old data:
        return self.create(data_date, new_swps)


class JSONSecurityWithPricesRepository(SecurityWithPricesRepository):
    # TODO_CLEANUP: retire this class if not being used?

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
    # TODO_CLEANUP: retire this class if not being used?
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


class APXIMEXLatestLogFileRepository:
    imex_log_folder_repo = APXDBIMEXLogFolderRepository()

    def get(self, login: str=None) -> Tuple[str, str, list]:

        # Get what folder contains the IMEX log files for this login
        log_folder = self.imex_log_folder_repo.get(login)

        # Find latest log file
        latest_log_file = self.get_latest_log_file(log_folder)

        # Now read that file 
        with open(latest_log_file, 'r', encoding='utf-16-le') as f:
            contents = f.read()

        # Trim starting char of contents - expected as '\ufeff' byte-order-mark
        contents = contents[1:]

        # Get errors
        logging.info(f'Searching the following IMEX log file for errors: {latest_log_file}')
        errors = self.get_errors(contents)

        # Return full path to log file, its contents, and any errors found
        return (latest_log_file, contents, errors)  

    def get_latest_log_file(self, log_folder: str) -> str:
        # Initialize variables to store information about the most recent log file
        most_recent_file = None
        most_recent_timestamp = 0

        # Regex pattern to identify IMEX log files
        pattern = r'^\d{8}-\d{6}\.log$'

        # Compile the regular expression pattern for efficiency
        regex = re.compile(pattern)

        # Iterate through files in the specified directory
        for filename in os.listdir(log_folder):
            # Check if the file matches the expected format using re.match
            if not regex.match(filename):
                continue
            try:
                timestamp = os.path.getmtime(os.path.join(log_folder, filename))
                # Update the most recent file if necessary
                if timestamp > most_recent_timestamp:
                    most_recent_timestamp = timestamp
                    most_recent_file = filename
            except (ValueError, OSError):
                continue

        # If a most recent file was found, return its full path
        if most_recent_file:
            return os.path.join(log_folder, most_recent_file)
        else:
            return None

    def get_errors(self, contents: str) -> list:

        # Split into lines
        logging.debug(f'Searching IMEX log file for errors... {contents}')
        lines = contents.split('\n')
        logging.debug(f'Split into {len(lines)} lines')

        # Search for ERROR lines
        errors = []
        for line_num, line in enumerate(lines):
            if line.startswith('ERROR'):
                # Found error line! Append it along with the following line, which should contain 
                # the error row from the file that was loaded via IMEX
                logging.info(f'Found ERROR on line {line_num}: {line}')
                errors.append(line + '\n' + lines[line_num+1])
        
        return errors

