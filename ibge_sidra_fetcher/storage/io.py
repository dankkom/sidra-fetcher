import datetime as dt
import logging
from pathlib import Path

from ..api.sidra.agregado import (
    Agregado,
    Categoria,
    Classificacao,
    Localidade,
    Periodo,
    Pesquisa,
    Variavel,
)
from . import locus, raw

logger = logging.getLogger(__name__)


def read_periodos(filepath: Path) -> list[Periodo]:
    logger.debug(f"Reading Periodos file {filepath}")
    periodos = []
    parse_modificacao = lambda x: dt.datetime.strptime(x, "%d/%m/%Y").date()
    for per in raw.read_json(filepath):
        p = Periodo(
            id=int(per["id"]),
            literals=per["literals"],
            modificacao=parse_modificacao(per["modificacao"]),
        )
        periodos.append(p)
    return periodos


def read_localidades(filepath: Path) -> list[Localidade]:
    logger.debug(f"Reading Localidades file {filepath}")
    localidades = []
    for loc in raw.read_json(filepath):
        l = Localidade(
            id=int(loc["id"]),
            nome=loc["nome"],
            id_nivel=loc["nivel"]["id"],
            nome_nivel=loc["nivel"]["nome"],
        )
        localidades.append(l)
    return localidades


def read_metadados(datadir: Path, pesquisa_id: str, agregado_id: int) -> Agregado:
    logger.debug(f"Reading Metadata {pesquisa_id}/{agregado_id}")

    files = {
        "metadados": locus.agregado_metadados_filepath(
            datadir=datadir,
            pesquisa_id=pesquisa_id,
            agregado_id=agregado_id,
        ),
        "localidades": list(
            locus.agregado_metadata_dir(
                datadir=datadir,
                pesquisa_id=pesquisa_id,
                agregado_id=agregado_id,
            ).glob("localidades-*.json")
        ),
        "periodos": locus.agregado_periodos_filepath(
            datadir=datadir,
            pesquisa_id=pesquisa_id,
            agregado_id=agregado_id,
        ),
    }

    localidades = []
    for f in files["localidades"]:
        localidades.extend(read_localidades(f))
    periodos = read_periodos(files["periodos"])
    data = raw.read_json(files["metadados"])
    variaveis = [
        Variavel(id=int(v["id"]), nome=v["nome"], unidade=v["unidade"])
        for v in data["variaveis"]
    ]
    classificacoes = [
        Classificacao(
            id=cla["id"],
            nome=cla["nome"],
            categorias=[
                Categoria(
                    id=int(cat["id"]),
                    nome=cat["nome"],
                    unidade=cat["unidade"],
                    nivel=int(cat["nivel"]),
                )
                for cat in cla["categorias"]
            ],
        )
        for cla in data["classificacoes"]
    ]
    a = Agregado(
        id=int(data["id"]),
        nome=data["nome"],
        url=data["URL"],
        pesquisa=Pesquisa(id=pesquisa_id, nome=data["pesquisa"]),
        assunto=data["assunto"],
        periodicidade_frequencia=data["periodicidade"]["frequencia"],
        variaveis=variaveis,
        classificacoes=classificacoes,
        periodos=periodos,
        localidades=localidades,
    )
    return a
