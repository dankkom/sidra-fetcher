import time

import httpx
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

import ibge_sidra_fetcher.api.agregados

from . import logger


class Fetcher:
    def __init__(self, client: httpx.Client) -> None:
        self.client = client

    def get(self, url: str) -> bytes:
        logger.info(f"Downloading DATA {url}")
        t0 = time.time()
        data = b""
        with self.client.stream("GET", url) as r:
            r.raise_for_status()
            for chunk in r.iter_bytes():
                data += chunk
        if not data:
            raise ConnectionError("Data returned is None!")
        t1 = time.time()
        logger.debug(f"Download of {url} took {t1 - t0:.2f} seconds")
        return data

    @retry(stop=stop_after_attempt(3))
    def get_agregados(self) -> bytes:
        url_agregados = ibge_sidra_fetcher.api.agregados.build_url_agregados()
        logger.info(f"Downloading list of agregados metadata {url_agregados}")
        data = self.get(url_agregados)
        return data

    @retry(stop=stop_after_attempt(3))
    def get_agregado_metadados(self, agregado_id: int) -> bytes:
        url_metadados = ibge_sidra_fetcher.api.agregados.build_url_metadados(
            agregado_id
        )
        logger.info(f"Downloading agregado metadados {url_metadados}")
        data = self.get(url_metadados)
        return data

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=3, max=30),
    )
    def get_agregado_periodos(self, agregado_id: int) -> bytes:
        url_periodos = ibge_sidra_fetcher.api.agregados.build_url_periodoso(
            agregado_id
        )
        logger.info(f"Downloading agregado periodos {url_periodos}")
        data = self.get(url_periodos)
        return data

    def get_agregado_localidades(
        self, agregado_id: int, localidades_nivel: str
    ) -> bytes:
        url_localidades = (
            ibge_sidra_fetcher.api.agregados.build_url_localidades(
                agregado_id, localidades_nivel
            )
        )
        logger.info(f"Downloading agregado localidades {url_localidades}")
        data = self.get(url_localidades)
        return data

    def get_acervo(self, acervo_id: str) -> bytes:
        url_acervo = ibge_sidra_fetcher.api.agregados.build_url_acervos(
            acervo_id
        )
        logger.info(f"Downloading acervo {url_acervo}")
        data = self.get(url_acervo)
        return data

    def __enter__(self) -> "Fetcher":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if hasattr(self.client, "close"):
            self.client.close()
        else:
            logger.warning("Client does not have a close method.")
        logger.info("Fetcher closed.")
