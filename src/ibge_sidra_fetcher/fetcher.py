import datetime as dt
import json
import time
from typing import Any

import httpx
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from ibge_sidra_fetcher.api.agregados import (
    AcervoEnum,
    Agregado,
    AgregadoNivelTerritorial,
    Categoria,
    Classificacao,
    ClassificacaoSumarizacao,
    IndiceAgregado,
    IndicePesquisaAgregados,
    Localidade,
    NivelTerritorial,
    Periodicidade,
    Periodo,
    Pesquisa,
    Variavel,
    build_url_acervos,
    build_url_agregados,
    build_url_localidades,
    build_url_metadados,
    build_url_periodos,
)

from . import logger


class Fetcher:
    def __init__(self) -> None:
        self.client = httpx.Client()

    def get(self, url: str) -> Any:
        """Fetch data from the given URL.
        Args:
            url (str): The URL to fetch data from.
        Returns:
            Any: The data fetched from the URL, parsed as JSON.
        Raises:
            ConnectionError: If the data returned is None or if the request fails.
        """
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
        data = json.loads(data.decode("utf-8"))
        return data

    @retry(stop=stop_after_attempt(3))
    def get_indice_pesquisas_agregados(self) -> list[IndicePesquisaAgregados]:
        """Fetch the list of agregados metadata."""
        url_agregados = build_url_agregados()
        logger.info(f"Downloading list of agregados metadata {url_agregados}")
        data = self.get(url_agregados)
        data = [
            IndicePesquisaAgregados(
                id=item["id"],
                nome=item["nome"],
                agregados=[
                    IndiceAgregado(
                        id=agregado["id"],
                        nome=agregado["nome"],
                    )
                    for agregado in item["agregados"]
                ],
            )
            for item in data
        ]
        return data

    @retry(stop=stop_after_attempt(3))
    def get_agregado_metadados(self, agregado_id: int) -> Agregado:
        """Fetch the metadata for a specific agregado by its ID."""
        url_metadados = build_url_metadados(agregado_id)
        logger.info(f"Downloading agregado metadados {url_metadados}")
        data = self.get(url_metadados)
        nivel_territorial = AgregadoNivelTerritorial(
            administrativo=data["nivelTerritorial"]["Administrativo"],
            especial=data["nivelTerritorial"]["Especial"],
            ibge=data["nivelTerritorial"]["IBGE"],
        )
        variaveis = [
            Variavel(
                id=v["id"],
                nome=v["nome"],
                unidade=v["unidade"],
                sumarizacao=v["sumarizacao"],
            )
            for v in data["variaveis"]
        ]
        classificacoes = [
            Classificacao(
                id=cla["id"],
                nome=cla["nome"],
                sumarizacao=ClassificacaoSumarizacao(
                    status=cla["sumarizacao"]["status"],
                    excecao=cla["sumarizacao"]["excecao"],
                ),
                categorias=[
                    Categoria(
                        id=cat["id"],
                        nome=cat["nome"],
                        unidade=cat["unidade"],
                        nivel=cat["nivel"],
                    )
                    for cat in cla["categorias"]
                ],
            )
            for cla in data["classificacoes"]
        ]
        agregado = Agregado(
            id=data["id"],
            nome=data["nome"],
            url=data["URL"],
            pesquisa=Pesquisa(id="", nome=data["pesquisa"]),
            assunto=data["assunto"],
            periodicidade=Periodicidade(**data["periodicidade"]),
            nivel_territorial=nivel_territorial,
            variaveis=variaveis,
            classificacoes=classificacoes,
            periodos=[],
            localidades=[],
        )
        return agregado

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=3, max=30),
    )
    def get_agregado_periodos(self, agregado_id: int) -> list[Periodo]:
        """Fetch the periods associated with a specific agregado by its ID."""
        url_periodos = build_url_periodos(agregado_id)
        logger.info(f"Downloading agregado periodos {url_periodos}")
        data = self.get(url_periodos)
        data = [
            Periodo(
                id=periodo["id"],
                literals=periodo["literals"],
                modificacao=dt.datetime.strptime(
                    periodo["modificacao"], "%d/%m/%Y"
                ).date(),
            )
            for periodo in data
        ]
        return data

    def get_agregado_localidades(
        self, agregado_id: int, localidades_nivel: str
    ) -> list[Localidade]:
        """Fetch the localidades associated with a specific agregado by its ID and level."""
        url_localidades = build_url_localidades(agregado_id, localidades_nivel)
        logger.info(f"Downloading agregado localidades {url_localidades}")
        data = self.get(url_localidades)
        return [
            Localidade(
                id=localidade["id"],
                nome=localidade["nome"],
                nivel=NivelTerritorial(
                    id=localidade["nivel"]["id"],
                    nome=localidade["nivel"]["nome"],
                ),
            )
            for localidade in data
        ]

    def get_agregado(self, agregado_id: int) -> Agregado:
        """Fetch the complete data for a specific agregado by its ID."""
        logger.info(f"Downloading agregado {agregado_id}")
        agregado_metadados = self.get_agregado_metadados(agregado_id)
        agregado_periodos = self.get_agregado_periodos(agregado_id)
        agregado_localidades: list[Localidade] = []
        for nivel in agregado_metadados.nivel_territorial.administrativo:
            localidades = self.get_agregado_localidades(agregado_id, nivel)
            agregado_localidades.extend(localidades)
        for nivel in agregado_metadados.nivel_territorial.especial:
            localidades = self.get_agregado_localidades(agregado_id, nivel)
            agregado_localidades.extend(localidades)
        for nivel in agregado_metadados.nivel_territorial.ibge:
            localidades = self.get_agregado_localidades(agregado_id, nivel)
            agregado_localidades.extend(localidades)
        agregado_metadados.periodos = agregado_periodos
        agregado_metadados.localidades = agregado_localidades
        return agregado_metadados

    def get_acervo(self, acervo: AcervoEnum) -> Any:
        """Fetch data from a specific acervo."""
        url_acervo = build_url_acervos(acervo)
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
