import random
import time
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import Generator

import httpx
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from . import api, logger
from .config import DATA_DIR, HTTP_HEADERS
from .storage import (
    agregado_localidades_filepath,
    agregado_metadados_filepath,
    agregado_periodos_filepath,
    write_data,
)


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


# DISPATCH --------------------------------------------------------------------
def metadados(pesquisa_id: str, agregado_id: int) -> dict[str, str | Path]:
    url_metadados = api.agregados.url.metadados(agregado_id=agregado_id)
    dest_filepath = agregado_metadados_filepath(
        datadir=DATA_DIR,
        pesquisa_id=pesquisa_id,
        agregado_id=agregado_id,
    )
    return {
        "url": url_metadados,
        "dest_filepath": dest_filepath,
    }


def periodos(pesquisa_id: str, agregado_id: int) -> dict[str, str | Path]:
    url_periodos = api.agregados.url.periodos(agregado_id=agregado_id)
    dest_filepath = agregado_periodos_filepath(
        datadir=DATA_DIR,
        pesquisa_id=pesquisa_id,
        agregado_id=agregado_id,
    )
    return {
        "url": url_periodos,
        "dest_filepath": dest_filepath,
    }


def localidades(
    pesquisa_id: str,
    agregado_id: int,
    localidades_nivel: str,
) -> dict[str, str | Path]:
    url_localidades = api.agregados.url.localidades(
        agregado_id=agregado_id,
        localidades_nivel=localidades_nivel,
    )
    dest_filepath = agregado_localidades_filepath(
        datadir=DATA_DIR,
        pesquisa_id=pesquisa_id,
        agregado_id=agregado_id,
        localidades_nivel=localidades_nivel,
    )
    return {
        "url": url_localidades,
        "dest_filepath": dest_filepath,
    }


def agregado_localidades(
    pesquisa_id: str, metadados: dict
) -> Generator[dict[str, str | Path], None, None]:
    """Fetches Localidades metadata, expects Metadados file in data directory"""

    def get_niveis(metadados: dict) -> set[str]:
        nivel_territorial = metadados["nivelTerritorial"]
        administrativo = nivel_territorial["Administrativo"]
        especial = nivel_territorial["Especial"]
        ibge = nivel_territorial["IBGE"]
        return set(administrativo + especial + ibge)

    agregado_id = metadados["id"]
    niveis = get_niveis(metadados)
    for nivel in niveis:
        yield localidades(
            pesquisa_id=pesquisa_id,
            agregado_id=agregado_id,
            localidades_nivel=nivel,
        )
