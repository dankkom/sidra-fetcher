"""Classes & functions to request resources from IBGE's SIDRA APIs

- https://apisidra.ibge.gov.br
- https://apisidra.ibge.gov.br/home/ajuda

"""


import logging

import httpx
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from ..config import HTTP_HEADERS, TIMEOUT

logger = logging.getLogger(__name__)


class SIDRAException(Exception):

    def __init__(self, msg) -> None:
        self.msg = msg
        logger.exception(msg)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=30))
def get_sidra_data(
    sidra_url: str,
    c: httpx.Client = None,
) -> bytes:
    logger.info(f"Downloading SIDRA {sidra_url}")
    if c is not None:
        r = c.get(sidra_url, headers=HTTP_HEADERS, timeout=TIMEOUT, verify=False)
    else:
        r = httpx.get(sidra_url, headers=HTTP_HEADERS, timeout=TIMEOUT, verify=False)
    if r.status_code != 200:
        raise SIDRAException(f"Error status code {r.status_code}\n{r.text}")
    data = r.content
    if data is None:
        raise SIDRAException("Data returned is None!")
    return data
