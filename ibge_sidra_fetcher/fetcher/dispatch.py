from pathlib import Path

from ..api import url
from ..config import DATA_DIR
from ..storage import locus


def metadados(pesquisa_id: str, agregado_id: int) -> dict[str, str | Path]:
    url_metadados = url.metadados(agregado_id=agregado_id)
    dest_filepath = locus.agregado_metadados_filepath(
        datadir=DATA_DIR,
        pesquisa_id=pesquisa_id,
        agregado_id=agregado_id,
    )
    return {
        "url": url_metadados,
        "dest_filepath": dest_filepath,
    }


def periodos(pesquisa_id: str, agregado_id: int) -> dict[str, str | Path]:
    url_periodos = url.periodos(agregado_id=agregado_id)
    dest_filepath = locus.agregado_periodos_filepath(
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
    url_localidades = url.localidades(
        agregado_id=agregado_id,
        localidades_nivel=localidades_nivel,
    )
    dest_filepath = locus.agregado_localidades_filepath(
        datadir=DATA_DIR,
        pesquisa_id=pesquisa_id,
        agregado_id=agregado_id,
        localidades_nivel=localidades_nivel,
    )
    return {
        "url": url_localidades,
        "dest_filepath": dest_filepath,
    }


def agregado_localidades(pesquisa_id: str, metadados: dict) -> dict[str, str | Path]:
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
