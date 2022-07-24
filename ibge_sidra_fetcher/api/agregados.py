"""Functions to request resources from IBGE's agregados APIs

- https://servicodados.ibge.gov.br/api/docs/agregados?versao=3

"""


import logging

import httpx
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

logger = logging.getLogger(__name__)

URL = "https://servicodados.ibge.gov.br/api/v3/agregados"


@retry(stop=stop_after_attempt(3))
def get_agregados() -> bytes:
    url = URL
    logger.info(f"Downloading list of agregados metadata {url}")
    r = httpx.get(url, verify=False)
    data = r.content
    return data


@retry(stop=stop_after_attempt(3))
def get_agregado_metadados(
    agregado_id: int,
    c: httpx.Client = None,
) -> bytes:
    url = URL + f"/{agregado_id}/metadados"
    logger.info(f"Downloading agregado metadados {url}")
    if c is not None:
        r = c.get(url)
    else:
        r = httpx.get(url, verify=False)
    data = r.content
    return data


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=30))
def get_agregado_periodos(
    agregado_id: int,
    c: httpx.Client = None,
) -> bytes:
    url = URL + f"/{agregado_id}/periodos"
    logger.info(f"Downloading agregado periodos {url}")
    if c is not None:
        r = c.get(url)
    else:
        r = httpx.get(url, verify=False)
    data = r.content
    return data


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=30))
def get_agregado_localidades(
    agregado_id: int,
    localidade_niveis: list[str],
    c: httpx.Client = None,
) -> bytes:
    localidades = []
    for nivel in localidade_niveis:
        url = URL + f"/{agregado_id}/localidades/{nivel}"
        logger.info(f"Downloading agregado localidades {url}")
        if c is not None:
            r = c.get(url)
        else:
            r = httpx.get(url, verify=False)
        localidades.extend(r.json())
    return localidades
