import time
import httpx
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential
from ..config import HTTP_HEADERS, TIMEOUT
from .. import logger


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=2, min=10, max=120),
)
def get(
    url: str,
    client: httpx.Client,
) -> bytes:
    logger.info(f"Downloading DATA {url}")
    get_args = {
        "headers": HTTP_HEADERS,
        "timeout": TIMEOUT,
        "verify": False,
    }
    data = b""
    t0 = time.time()
    with client.stream("GET", url, **get_args) as r:
        r.raise_for_status()
        for chunk in r.iter_bytes():
            data += chunk
    if data is None:
        raise ConnectionError("Data returned is None!")
    t1 = time.time()
    logger.debug(f"Download of {url} took {t1 - t0:.2f} seconds")
    return data
