import logging

import httpx
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from ..api import agregados
from ..config import HTTP_HEADERS

logger = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=2, min=10, max=120),
)
def get(
    url: str,
    client: httpx.Client,
) -> bytes:
    logger.info(f"Downloading DATA {url}")
    data = b""
    with client.stream("GET", url, headers=HTTP_HEADERS, verify=False) as r:
        if r.status_code != 200:
            raise ConnectionError(f"Error status code {r.status_code}\n{r.text}")
        for chunk in r.iter_bytes():
            data += chunk
    if data is None:
        raise ConnectionError("Data returned is None!")
    return data


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=2, min=10, max=120),
)
def sidra_agregados(client: httpx.Client) -> bytes:
    data = agregados.get_agregados(client)
    return data


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=2, min=10, max=120),
)
def sidra_data(
    sidra_url: str,
    client: httpx.Client = None,
) -> bytes:
    logger.info(f"Downloading SIDRA {sidra_url}")
    data = b""
    with client.stream("GET", sidra_url) as r:
        if r.status_code != 200:
            raise ConnectionError(f"Error status code {r.status_code}\n{r.text}")
        for chunk in r.iter_bytes():
            data += chunk
    if data is None:
        raise ConnectionError("Data returned is None!")
    return data
