
# core python
import logging
from typing import List, Optional, Union, Tuple

class DefaultRESTFormatter:
    # TODO: Create a multi-repo post formatter, e.g. for PriceByIMEX, 
    # to provide a full summary of inserts to mutliple repo's

    def success_get(self, data: Union[dict, list]) -> Tuple[dict, int]:
        return {
            'data': data,
            'message': None,
            'status': 'success',
        }, 200
    
    def success_post(self, row_cnt: int) -> Tuple[dict, int]:
        if row_cnt:
            return {
                'data': None,
                'message': f"Successfully saved {row_cnt} row{'' if row_cnt == 1 else 's'}.",
                'status': 'success',
            }, 201
        else:
            return {
                'data': None,
                'message': f"Succeeded, but nothing was saved.",
                'status': 'warning',
            }, 200

    def success_delete(self, row_cnt: int) -> Tuple[dict, int]:
        if row_cnt:
            return {
                'data': None,
                'message': f"Successfully deleted {row_cnt} row{'' if row_cnt == 1 else 's'}.",
                'status': 'success',
            }, 201
        else:
            return {
                'data': None,
                'message': f"Succeeded, but found nothing to delete.",
                'status': 'warning',
            }, 200

    def exception(self, e: Exception, http_return_code: int=500) -> Tuple[dict, int]:
        logging.exception(f'Returning {http_return_code} due to {type(e).__name__}: {e}')
        return {
            'data': f'{type(e).__name__}: {e}',
            'message': f'{type(e).__name__}: {e}',
            'status': 'error', 
        }, http_return_code

