
# core python
from typing import List, Optional, Union, Tuple

class DefaultRESTFormatter:
    def success_get(self, data: Union[dict, list]) -> Tuple[dict, int]:
        return {
            'data': data,
            'message': None,
            'status': 'success',
        }, 200
    
    def success_post(self, data: Union[dict, list], message: Optional[str]=None) -> Tuple[dict, int]:
        return {
            'data': data,
            'message': message,
            'status': 'success',
        }, 201

    def exception(self, e: Exception, http_return_code: int=500) -> Tuple[dict, int]:
        return {
            'data': None,
            'message': f'{type(e).__name__}: {e}',
            'status': 'error', 
        }, http_return_code

