import httpx
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from ... import logger
from ...config import HTTP_HEADERS, TIMEOUT
from . import url


@retry(stop=stop_after_attempt(3))
def get_agregados(c: httpx.Client = None) -> bytes:
    url_agregados = url.agregados()
    logger.info(f"Downloading list of agregados metadata {url_agregados}")
    if c is not None:
        r = c.get(url_agregados, headers=HTTP_HEADERS)
    else:
        r = httpx.get(
            url_agregados, headers=HTTP_HEADERS, timeout=TIMEOUT, verify=False
        )
    data = r.content
    return data


@retry(stop=stop_after_attempt(3))
def get_metadados(
    agregado_id: int,
    c: httpx.Client = None,
) -> bytes:
    url_metadados = url.metadados(agregado_id)
    logger.info(f"Downloading agregado metadados {url_metadados}")
    if c is not None:
        r = c.get(url_metadados, headers=HTTP_HEADERS)
    else:
        r = httpx.get(
            url_metadados, headers=HTTP_HEADERS, timeout=TIMEOUT, verify=False
        )
    data = r.content
    return data


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=30))
def get_periodos(
    agregado_id: int,
    c: httpx.Client = None,
) -> bytes:
    url_periodos = url.periodos(agregado_id)
    logger.info(f"Downloading agregado periodos {url_periodos}")
    if c is not None:
        r = c.get(url_periodos, headers=HTTP_HEADERS)
    else:
        r = httpx.get(url_periodos, headers=HTTP_HEADERS, timeout=TIMEOUT, verify=False)
    data = r.content
    return data


def get_localidades(
    agregado_id: int,
    localidades_nivel: str,
    c: httpx.Client = None,
) -> bytes:
    url_localidades = url.localidades(agregado_id, localidades_nivel)
    logger.info(f"Downloading agregado localidades {url_localidades}")
    if c is not None:
        r = c.get(url_localidades, headers=HTTP_HEADERS)
    else:
        r = httpx.get(
            url_localidades, headers=HTTP_HEADERS, timeout=TIMEOUT, verify=False
        )
    localidades = r.json()
    return localidades
