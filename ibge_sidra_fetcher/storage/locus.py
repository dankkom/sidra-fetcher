"""

DATA_DIR/<pesquisa_id>/agregado-<agregado_id>/metadados.json
DATA_DIR/<pesquisa_id>/agregado-<agregado_id>/periodos.json
DATA_DIR/<pesquisa_id>/agregado-<agregado_id>/localidades-<localidades_nivel_id*>.json

"""


import datetime as dt
from pathlib import Path


def sidra_agregados_filepath(datadir: Path) -> Path:
    return datadir / "sidra-agregados.json"


def pesquisa_dir(datadir: Path, pesquisa_id: str) -> Path:
    return datadir / pesquisa_id.lower()


def agregado_dir(datadir: Path, pesquisa_id: str, agregado_id: int) -> Path:
    return pesquisa_dir(datadir, pesquisa_id) / f"agregado-{agregado_id:05}"


def agregado_metadata_dir(
    datadir: Path,
    pesquisa_id: str,
    agregado_id: int,
) -> Path:
    return agregado_dir(datadir, pesquisa_id, agregado_id) / "metadata"


def agregado_metadados_filepath(
    datadir: Path,
    pesquisa_id: str,
    agregado_id: int,
) -> Path:
    return (
        agregado_metadata_dir(
            datadir,
            pesquisa_id,
            agregado_id,
        )
        / "metadados.json"
    )


def agregado_periodos_filepath(
    datadir: Path,
    pesquisa_id: str,
    agregado_id: int,
) -> Path:
    return (
        agregado_metadata_dir(
            datadir,
            pesquisa_id,
            agregado_id,
        )
        / "periodos.json"
    )


def agregado_localidades_filepath(
    datadir: Path,
    pesquisa_id: str,
    agregado_id: int,
    localidades_nivel: str,
) -> Path:
    metadata_dir = agregado_metadata_dir(
        datadir,
        pesquisa_id,
        agregado_id,
    )
    return metadata_dir / f"localidades-{localidades_nivel.lower()}.json"


def data_filepath(
    datadir: Path,
    pesquisa_id: str,
    agregado_id: int,
    periodo_id: int,
    modificacao: dt.date,
    localidade_id: int = None,
    variavel_id: int = None,
) -> Path:
    d = agregado_dir(datadir, pesquisa_id, agregado_id)
    partition = [f"{periodo_id}"]
    if localidade_id:
        partition.append(f"{localidade_id}")
    if variavel_id:
        partition.append(f"{variavel_id}")
    partition = "-".join(partition)
    filename = f"{agregado_id}_{partition}_{modificacao:%Y%m%d}.json"
    return d / filename
