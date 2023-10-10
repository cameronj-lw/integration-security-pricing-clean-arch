
# core python
import datetime
import logging
import os
import requests
import socket
from typing import List, Optional, Tuple, Union

# pypi
import pandas as pd
from sqlalchemy import exc, update, and_

# native
from app.domain.models import Price, PriceType, PriceSource, PriceValue, Security
from app.domain.repositories import PriceRepository
from app.infrastructure.sql_repositories import CoreDBSecurityRepository
from app.infrastructure.sql_tables import APXDBvPriceTable
from app.infrastructure.util.config import AppConfig
from app.infrastructure.util.file import prepare_dated_file_path



class APXPriceType(PriceType):
    """ Subclass of PriceType to add additional APX-specific attributes """

    def __init__(self, price_type: PriceType):
        super().__init__(price_type.name)

        # Populate additional APX-specific attributes below
        if self.name == 'price':
            self.price_type_id = 1  # Standard Prices
            self.imex_file_suffix = ''
        elif self.name == 'yield':
            self.price_type_id = 2  # LW Bond Yield
            self.imex_file_suffix = '_LWBondYield'
        elif self.name == 'duration':
            self.price_type_id = 3  # LW Bond Duration
            self.imex_file_suffix = '_LWBondDur'
        else:
            raise NotImplementedError(f"Cannot create APXPriceType with name {self.name}")

    def __hash__(self):
        return hash(self.name)


class APXPriceSource(PriceSource):
    """ Subclass of PriceSource to add additinoal APX-specific attributes """

    def __init__(self, price_source: PriceSource):
        super().__init__(price_source.name)

        # Populate additional APX-specific attributes below
        if self.name in ('FTSE', 'FTSETMX_PX'):
            self.price_source_id = 3006  # LW FTSE TMX
        elif self.name == 'BLOOMBERG':
            self.price_source_id = 3004  # LW Bloomberg
        elif self.name == 'MARKIT':
            self.price_source_id = 3005  # LW Markit
        elif self.name == 'MARKIT_LOAN':
            self.price_source_id = 3011  # LW Markit Loan
        elif self.name == 'FUNDRUN':
            self.price_source_id = 3019  # LW Fundrun Equity
        elif self.name == 'FIDESK_MANUALPRICE':
            self.price_source_id = 3007  # LW FI Desk - Manual Price
        elif self.name == 'FIDESK_MISSINGPRICE':
            self.price_source_id = 3008  # LW FI Desk - Missing Price
        elif self.name == 'MISSING':
            self.price_source_id = 3008  # LW FI Desk - Missing Price
        elif self.name == 'MANUAL':
            self.price_source_id = 3032  # LW Security Pricing - Manual
        elif self.name == 'OVERRIDE':
            self.price_source_id = 3033  # LW Security Pricing - Override
        else:
            self.price_source_id = 3000  # LW Not Classified


class IMEXError(Exception):
    pass


class APXPriceRepository(PriceRepository):
    # We'll retrieve security attributes from below repo
    security_repo = CoreDBSecurityRepository()

    def create(self, prices: Union[List[Price], Price]) -> int:
        if isinstance(prices, Price):
            # Need to convert to List in order to loop thru
            prices = [prices]

        # Loop thru and populate a dict by price date and type, since for IMEX we'll create one file per date & type.
        prices_by_date_and_type = {}
        for px in prices:
            # Skip anything not from LW price sources.
            # Rationale is: For vendor sources, they will have already been loaded into APX by the pre-existing loaders.
            # And if there is no LW user pricing them, there is no audit trail requirement.
            lw_price_source_names = AppConfig().parser.get('app', 'lw_price_sources').split(',')
            lw_price_source_names = [s.strip() for s in lw_price_source_names]
            if px.source.name not in lw_price_source_names:
                continue

            # Convert PriceTypes to APXPriceTypes. This will populate additional APX-specific attributes.
            # px.values = [PriceValue(APXPriceType(pv.type_), pv.value) for pv in px.values]

            # Get Security with attributes, and replace the Security in the Price.
            # Need to do this because the Security as part of the price likely only has an lw_id, 
            # and we need more attributes for IMEX.
            sec = self.security_repo.get(px.security.lw_id)[0]
            px.security = sec

            # Add date to top level, if it DNE
            if px.data_date not in prices_by_date_and_type:
                prices_by_date_and_type[px.data_date] = {}

            # Loop through values, and add them to the dict
            for price_value in px.values:
                # Get APXPriceType. This will provide additional APX-specific attributes.
                apx_price_type = APXPriceType(price_value.type_)

                # Create empty DataFrame under data_date and apx_price_type, if it DNE
                if apx_price_type not in prices_by_date_and_type[px.data_date]:
                    prices_by_date_and_type[px.data_date][apx_price_type] = pd.DataFrame(
                            columns=['pms_sec_type','pms_symbol','value','message','source'])
                    
                # Get IMEX fields dict, then append to DataFrame
                imex_fields_dict = self.get_imex_price_fields_dict(px, price_value)
                prices_by_date_and_type[px.data_date][apx_price_type] = (
                    pd.concat([prices_by_date_and_type[px.data_date][apx_price_type]
                            , pd.DataFrame([imex_fields_dict])], ignore_index=True)
                )

        # Now we have a layered dict of DataFrames as follows: {
        #   from_date: {
        #       apx_price_type: <DataFrame>
        #   }
        # }
        data_dir = AppConfig().parser.get('files', 'data_dir')
        base_path = os.path.join(data_dir, 'lw', 'security_pricing', 'imex')
        # Loop thru it and generate the files for IMEX.
        # Also append to a list of IMEX files as we go, so that we can then trigger IMEX fro each one
        imex_files = []
        for from_date in prices_by_date_and_type:
            for apx_price_type in prices_by_date_and_type[from_date]:
                # Get file name (APX IMEX format)
                file_name = f'{from_date.strftime("%m%d%y")}{apx_price_type.imex_file_suffix}.pri'

                # Get full path for the new file
                full_path = prepare_dated_file_path(folder_name=base_path, date=datetime.date.today()
                    , file_name=file_name, rotate=True)

                # Write to file
                df = prices_by_date_and_type[from_date][apx_price_type]
                df.to_csv(path_or_buf=full_path, sep='\t', header=False, index=False)
                imex_files.append(full_path)

        # Trigger IMEX
        imex_results = {}
        for full_path in imex_files:
            imex_response = self.trigger_imex_api(full_path)
            if not imex_response.ok:
                logging.error(f"{imex_response.json()['message']}")
                logging.error(f"Log result from {imex_response.json()['data']['imex_log_file']}: \n\n{imex_response.json()['data']['imex_log_file_contents']}")
                imex_results[full_path] = {
                    'http_status_code': imex_response.status_code,
                    'result': imex_response.json()
                }
                raise IMEXError(f'An IMEX command failed!')

        logging.info(f'APXPriceRepository returning {len(prices)}')
        return len(prices)  # return row count of the number successfully saved
        
    def trigger_imex(self, full_path, mode='Ama'):   
        # Get configs
        prefix = AppConfig().parser.get('apx_imex', 'apx_server')
        imex_base_url = AppConfig().parser.get('apx_imex', 'rest_api_base_url')

        # Build commands
        # regedit_cmd = f"C:\\Windows\\regedit /s {prefix}\\APX$\\exe\\ServerURL.reg"  # TODO_CLEANUP: is this needed? The IMEXUtil.pm does it before calling IMEX
        folder = os.path.dirname(full_path)     
        # TODO_CLEANUP: remove below? Not needed?
        folder = folder.replace('R:', '\\\\dev-data\\lws$')
        full_path = full_path.replace('R:', '\\\\dev-data\\lws$')
        imex_cmd = f"\\\\{prefix}\\APX$\\exe\\ApxIX.exe IMEX -i \"-s{folder}\" -{mode} \"-f{full_path}\" -ttab4 -u"
        
        payload = {'cmd': imex_cmd}
        logging.info(f'Submitting request for IMEX cmd to {imex_base_url}/run-cmd: {imex_cmd}')
        response = requests.post(f'{imex_base_url}/run-cmd', json=payload)
        logging.info(f'IMEX POST response: {response}')
        return response

    def trigger_imex_api(self, full_path, mode='merge_and_append'): 
        # Get configs
        imex_base_url = AppConfig().parser.get('apx_imex', 'rest_api_base_url')

        # Build payload and submit request to external IMEX REST API
        payload = {'full_path': full_path, 'mode': mode}
        logging.info(f'Submitting {mode} request for IMEX cmd to {imex_base_url}/run-imex with IMEX file: {full_path}')
        response = requests.post(f'{imex_base_url}/run-imex', json=payload)
        logging.info(f'IMEX POST response: {response}')
        return response

    def get_imex_price_fields_dict(self, px: Price, pv: PriceValue) -> dict:
        return {
            'pms_sec_type'  : px.security.attributes['pms_sec_type'],
            'pms_symbol'    : px.security.attributes['pms_symbol'],
            'value'         : pv.value,
            'message'       : '',
            'source'        : APXPriceSource(px.source).price_source_id,
        }

    def get(self, data_date: datetime.date, source: Union[PriceSource,None]=None
            , security: Union[Security,None]=None) -> List[Price]:

        pass


