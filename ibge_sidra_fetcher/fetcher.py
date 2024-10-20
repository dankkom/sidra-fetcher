import random
import time
from queue import Queue
from threading import Thread

import httpx
from tenacity.wait import wait_exponential

from .api.agregados import url as url_builder
from tenacity import retry
from tenacity.stop import stop_after_attempt

from . import logger
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
                logger.exception("Error %s %s", e, url)
                time.sleep(2 * random.random())
            finally:
                self.q.task_done()
            time.sleep(2 * random.random())


# GET -------------------------------------------------------------------------
def get(
    url: str,
    client: httpx.Client,
) -> bytes:
    logger.info(f"Downloading DATA {url}")
    t0 = time.time()
    data = b""
    with client.stream("GET", url) as r:
        r.raise_for_status()
        for chunk in r.iter_bytes():
            data += chunk
    if data is None:
        raise ConnectionError("Data returned is None!")
    t1 = time.time()
    logger.debug(f"Download of {url} took {t1 - t0:.2f} seconds")
    return data


@retry(stop=stop_after_attempt(3))
def get_agregados(c: httpx.Client) -> bytes:
    url_agregados = url_builder.agregados()
    logger.info(f"Downloading list of agregados metadata {url_agregados}")
    return get(url_agregados, c)


@retry(stop=stop_after_attempt(3))
def get_agregado_metadados(agregado_id: int, c: httpx.Client) -> bytes:
    url_metadados = url_builder.metadados(agregado_id)
    logger.info(f"Downloading agregado metadados {url_metadados}")
    return get(url_metadados, c)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=30))
def get_agregado_periodos(agregado_id: int, c: httpx.Client) -> bytes:
    url_periodos = url_builder.periodos(agregado_id)
    logger.info(f"Downloading agregado periodos {url_periodos}")
    return get(url_periodos, c)


def get_agregado_localidades(
    agregado_id: int, localidades_nivel: str, c: httpx.Client
) -> bytes:
    url_localidades = url_builder.localidades(agregado_id, localidades_nivel)
    logger.info(f"Downloading agregado localidades {url_localidades}")
    data = get(url_localidades, c)
    return data


def get_acervo(acervo_id: str, c: httpx.Client) -> bytes:
    url_acervo = url_builder.acervos(acervo_id)
    logger.info(f"Downloading acervo {url_acervo}")
    data = get(url_acervo, c)
    return data
