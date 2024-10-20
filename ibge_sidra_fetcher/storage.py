"""

File structure:

DATA_DIR/
    sidra-agregados.json
    <pesquisa_id>/
        agregado-<agregado_id>/
            metadata/
                metadados.json
                periodos.json
                localidades-<localidades_nivel_id*>.json
            <agregado_id>_<periodo_id>_<modificacao>.json
            <agregado_id>_<periodo_id>_<modificacao>.json
            ...
        agregado-<agregado_id>/
            metadata/
                metadados.json
                periodos.json
                localidades-<localidades_nivel_id*>.json
            <agregado_id>_<periodo_id>_<modificacao>.json
            <agregado_id>_<periodo_id>_<modificacao>.json
            ...
        ...
    <pesquisa_id>/
        agregado-<agregado_id>/
            metadata/
                metadados.json
                periodos.json
                localidades-<localidades_nivel_id*>.json
            <agregado_id>_<periodo_id>_<modificacao>.json
            <agregado_id>_<periodo_id>_<modificacao>.json
            ...
        agregado-<agregado_id>/
            metadata/
                metadados.json
                periodos.json
                localidades-<localidades_nivel_id*>.json
            <agregado_id>_<periodo_id>_<modificacao>.json
            <agregado_id>_<periodo_id>_<modificacao>.json
            ...
        ...
    ...

"""

import datetime as dt
import json
from pathlib import Path

from . import logger
from .api.agregados.agregado import (
    Agregado,
    Categoria,
    Classificacao,
    ClassificacaoSumarizacao,
    Localidade,
    LocalidadeNivel,
    NivelTerritorial,
    Periodicidade,
    Periodo,
    Pesquisa,
    Variavel,
)


# IO --------------------------------------------------------------------------
def read_periodos(filepath: Path) -> list[Periodo]:
    logger.debug(f"Reading Periodos file {filepath}")
    periodos = []
    parse_modificacao = lambda x: dt.datetime.strptime(x, "%d/%m/%Y").date()
    for per in read_json(filepath):
        p = Periodo(
            id=per["id"],
            literals=per["literals"],
            modificacao=parse_modificacao(per["modificacao"]),
        )
        periodos.append(p)
    return periodos


def read_localidades(filepath: Path) -> list[Localidade]:
    logger.debug(f"Reading Localidades file {filepath}")
    localidades = []
    for loc in read_json(filepath):
        l = Localidade(
            id=loc["id"],
            nome=loc["nome"],
            nivel=LocalidadeNivel(**loc["nivel"]),
        )
        localidades.append(l)
    return localidades


def read_metadados(data_dir: Path, pesquisa_id: str, agregado_id: int) -> Agregado:
    logger.debug(f"Reading Metadata {pesquisa_id}/{agregado_id}")

    files = {
        "metadados": agregado_metadados_filepath(
            data_dir=data_dir,
            agregado_id=agregado_id,
        ),
        "localidades": list(
            agregado_dir(
                data_dir=data_dir,
                agregado_id=agregado_id,
            ).glob("localidades-*.json")
        ),
        "periodos": agregado_periodos_filepath(
            data_dir=data_dir,
            agregado_id=agregado_id,
        ),
    }

    if not files["metadados"].exists():
        return

    localidades = []
    for f in files["localidades"]:
        if f.exists():
            localidades.extend(read_localidades(f))

    periodos = read_periodos(files["periodos"]) if files["periodos"].exists() else []

    data = read_json(files["metadados"])

    nivel_territorial = NivelTerritorial(
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
    a = Agregado(
        id=data["id"],
        nome=data["nome"],
        url=data["URL"],
        pesquisa=Pesquisa(id=pesquisa_id, nome=data["pesquisa"]),
        assunto=data["assunto"],
        periodicidade=Periodicidade(**data["periodicidade"]),
        nivel_territorial=nivel_territorial,
        variaveis=variaveis,
        classificacoes=classificacoes,
        periodos=periodos,
        localidades=localidades,
    )
    return a


# LOCUS -----------------------------------------------------------------------

# DATA_DIR/<pesquisa_id>/<agregado_id>/metadados.json
# DATA_DIR/<pesquisa_id>/<agregado_id>/periodos.json
# DATA_DIR/<pesquisa_id>/<agregado_id>/localidades-<localidades_nivel_id*>.json


def agregados_filepath(data_dir: Path) -> Path:
    return data_dir / "agregados.json"


def agregado_dir(data_dir: Path, agregado_id: int) -> Path:
    return data_dir / f"{agregado_id:0>6}"


def agregado_metadados_filepath(
    data_dir: Path,
    agregado_id: int,
) -> Path:
    return (
        agregado_dir(
            data_dir,
            agregado_id,
        )
        / "metadados.json"
    )


def agregado_periodos_filepath(
    data_dir: Path,
    agregado_id: int,
) -> Path:
    return (
        agregado_dir(
            data_dir,
            agregado_id,
        )
        / "periodos.json"
    )


def agregado_localidades_filepath(
    data_dir: Path,
    agregado_id: int,
    localidades_nivel: str,
) -> Path:
    return (
        agregado_dir(
            data_dir,
            agregado_id,
        )
        / f"localidades-{localidades_nivel.lower()}.json"
    )


def get_filename(
    sidra_tabela: str,
    periodo: str,
    territorial_level: str,
    ibge_territorial_code: str,
    variable: str = "allxp",
    classifications: dict[str, str] = None,
    data_modificacao: str = None,
):
    name = f"t-{sidra_tabela}_p-{periodo}"
    name += f"_n{territorial_level}-{ibge_territorial_code}"
    name += f"_v-{variable}"
    if classifications is not None:
        for classificacao, categoria in classifications.items():
            name += f"_c{classificacao}-{categoria}"
    name += f"@{data_modificacao}" if data_modificacao is not None else ""
    name += ".json"
    return name


def data_filepath(
    data_dir: Path,
    agregado_id: int,
    periodo_id: int,
    modificacao: dt.date,
    localidade_id: int = None,
    variavel_id: int = None,
) -> Path:
    d = agregado_dir(data_dir, agregado_id)
    partition = [f"{periodo_id}"]
    if localidade_id:
        partition.append(f"{localidade_id}")
    if variavel_id:
        partition.append(f"{variavel_id}")
    partition = "-".join(partition)
    filename = f"{agregado_id}_{partition}_{modificacao:%Y%m%d}.json"
    return d / filename


# RAW -------------------------------------------------------------------------
def read_json(filepath: Path):
    logger.info(f"Reading JSON file {filepath}")
    with filepath.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def write_json(data: dict | list | bytes, filepath: Path):
    logger.info(f"Writing JSON file {filepath}")
    if isinstance(data, bytes):
        data = json.loads(data.decode("utf-8"))
    filepath.parent.mkdir(exist_ok=True, parents=True)
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def write_data(data: bytes, filepath: Path):
    logger.info(f"Writing bytes file {filepath}")
    filepath.parent.mkdir(exist_ok=True, parents=True)
    with filepath.open("wb") as f:
        f.write(data)
