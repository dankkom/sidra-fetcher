import random
import time
from queue import Queue
from threading import Thread

import httpx
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from . import api, logger
from .config import HTTP_HEADERS
from .storage import write_data


class Fetcher(Thread):
    def __init__(self, q: Queue):
        super().__init__()
        self.daemon = True
        self.q = q

    def run(self):
        client = httpx.Client(timeout=300)
        while True:
            task = self.q.get()
            dest_filepath = task["dest_filepath"]
            url = task["url"]
            try:
                t0 = time.time()
                data = get(url, client)
                t1 = time.time()
                logger.debug(f"Download of {url} took {t1 - t0:.2f} seconds")
                write_data(data, dest_filepath)
            except Exception as e:
                print("Error", e)
                print("Error", url)
                time.sleep(2 * random.random())
            finally:
                self.q.task_done()
            time.sleep(2 * random.random())


# GET -------------------------------------------------------------------------
@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=2, min=10, max=120),
)
def get(
    url: str,
    client: httpx.Client,
) -> bytes:
    logger.info(f"Downloading DATA {url}")
    t0 = time.time()
    data = b""
    with client.stream("GET", url, headers=HTTP_HEADERS, verify=False) as r:
        if r.status_code != 200:
            raise ConnectionError(f"Error status code {r.status_code}\n{r.text}")
        for chunk in r.iter_bytes():
            data += chunk
    if data is None:
        raise ConnectionError("Data returned is None!")
    t1 = time.time()
    logger.debug(f"Download of {url} took {t1 - t0:.2f} seconds")
    return data


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=2, min=10, max=120),
)
def sidra_agregados(client: httpx.Client) -> bytes:
    data = api.agregados.handler.get_agregados(client)
    return data


def sidra_agregados_metadados(agregado_id: int, client: httpx.Client) -> bytes:
    data = api.agregados.handler.get_metadados(agregado_id, client)
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
    data = get(sidra_url, client)
    return data
