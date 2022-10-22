import logging
import time
import random
from queue import Queue
from threading import Thread
from pathlib import Path

import httpx
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from . import utils
from .api import agregados
from .config import DATA_DIR, HTTP_HEADERS, TIMEOUT
from .storage import (agregado_metadata_localidades_nivel_filepath, read_json,
                      write_json, write_data)

logger = logging.getLogger(__name__)


class SIDRAException(Exception):

    def __init__(self, msg) -> None:
        self.msg = msg
        logger.exception(msg)


class Fetcher(Thread):

    def __init__(self, q: Queue, data_dir: Path):
        super().__init__()
        self.daemon = True
        self.data_dir = data_dir
        self.q = q

    def run(self):
        client = httpx.Client()
        while True:
            task = self.q.get()
            dest_filepath = task["dest_filepath"]
            url = task["url"]
            try:
                t0 = time.time()
                data = get_sidra_data(url, client)
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


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=30))
def get_sidra_data(
    sidra_url: str,
    client: httpx.Client = None,
) -> bytes:
    logger.info(f"Downloading SIDRA {sidra_url}")
    if client is not None:
        r = client.get(sidra_url)
    else:
        r = httpx.get(
            sidra_url,
            headers=HTTP_HEADERS,
            timeout=TIMEOUT,
            verify=False,
        )
    if r.status_code != 200:
        raise SIDRAException(f"Error status code {r.status_code}\n{r.text}")
    data = r.content
    if data is None:
        raise SIDRAException("Data returned is None!")
    return data


def get_sidra_agregados(client: httpx.Client):
    output = DATA_DIR / "sidra-agregados.json"
    data = agregados.get_agregados(client)
    with open(output, "wb") as f:
        f.write(data)


def get_agregados_periodos(client: httpx.Client):
    for agregado in utils.iter_sidra_agregados(DATA_DIR):
        agregado_id = agregado["agregado_id"]
        output_file = agregado["metadata_files"]["periodos"]
        if output_file.exists():
            continue
        try:
            periodos = agregados.get_periodos(agregado_id, client)
            write_json(data=periodos, filepath=output_file)
        except Exception as e:
            print(e)
            time.sleep(5)


def get_agregados_metadados(client: httpx.Client):
    for agregado in utils.iter_sidra_agregados(DATA_DIR):
        agregado_id = agregado["agregado_id"]
        output_file = agregado["metadata_files"]["metadados"]
        if output_file.exists():
            continue
        try:
            agregado_metadata = agregados.get_metadados(agregado_id, client)
            write_json(data=agregado_metadata, filepath=output_file)
        except Exception as e:
            print(e)
            time.sleep(5)


def get_agregados_localidades(client: httpx.Client):

    def get_niveis(metadados):
        nivel_territorial = metadados["nivelTerritorial"]
        administrativo = nivel_territorial["Administrativo"]
        especial = nivel_territorial["Especial"]
        ibge = nivel_territorial["IBGE"]
        return set(administrativo + especial + ibge)

    for agregado in utils.iter_sidra_agregados(DATA_DIR):
        pesquisa_id = agregado["pesquisa_id"]
        agregado_id = agregado["agregado_id"]
        metadata_path = agregado["metadata_files"]
        metadados_filepath = metadata_path["metadados"]
        metadados = read_json(metadados_filepath)
        niveis = get_niveis(metadados)
        for nivel in niveis:
            output_file = agregado_metadata_localidades_nivel_filepath(
                datadir=DATA_DIR,
                pesquisa_id=pesquisa_id,
                agregado_id=agregado_id,
                localidades_nivel=nivel,
            )
            if output_file.exists():
                continue
            try:
                agregado_localidades = agregados.get_localidades(
                    agregado_id,
                    nivel,
                    client,
                )
                write_json(data=agregado_localidades, filepath=output_file)
            except Exception as e:
                print(e)
                time.sleep(5)
