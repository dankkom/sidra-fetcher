"""Di.apipatches agreg to fetch Agregados data"""

from pathlib import Path
from typing import Generator

from .api.agregados import (
    AcervoEnum,
    build_url_acervos,
    build_url_localidades,
    build_url_metadados,
    build_url_periodoso,
)
from .storage import (
    agregado_localidades_filepath,
    agregado_metadados_filepath,
    agregado_periodos_filepath,
)


# DISPATCH --------------------------------------------------------------------
def metadados(data_dir: Path, agregado_id: int) -> dict[str, str | Path]:
    url_metadados = build_url_metadados(agregado_id=agregado_id)
    dest_filepath = agregado_metadados_filepath(
        data_dir=data_dir,
        agregado_id=agregado_id,
    )
    return {
        "url": url_metadados,
        "dest_filepath": dest_filepath,
    }


def periodos(data_dir: Path, agregado_id: int) -> dict[str, str | Path]:
    url_periodos = build_url_periodoso(agregado_id=agregado_id)
    dest_filepath = agregado_periodos_filepath(
        data_dir=data_dir,
        agregado_id=agregado_id,
    )
    return {
        "url": url_periodos,
        "dest_filepath": dest_filepath,
    }


def localidades(
    data_dir: Path,
    agregado_id: int,
    localidades_nivel: str,
) -> dict[str, str | Path]:
    url_localidades = build_url_localidades(
        agregado_id=agregado_id,
        localidades_nivel=localidades_nivel,
    )
    dest_filepath = agregado_localidades_filepath(
        data_dir=data_dir,
        agregado_id=agregado_id,
        localidades_nivel=localidades_nivel,
    )
    return {
        "url": url_localidades,
        "dest_filepath": dest_filepath,
    }


def agregado_localidades(
    data_dir: Path,
    metadados: dict,
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
            data_dir=data_dir,
            agregado_id=agregado_id,
            localidades_nivel=nivel,
        )


def acervo(data_dir: Path, acervo: AcervoEnum) -> dict[str, str | Path]:
    url_acervo = build_url_acervos(acervo_id=acervo.value)
    dest_filepath = data_dir / f"{acervo.value}.json"
    return {
        "url": url_acervo,
        "dest_filepath": dest_filepath,
    }
