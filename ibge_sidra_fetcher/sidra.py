import datetime as dt
from dataclasses import dataclass
from pathlib import Path

from .storage import read_json, agregado_metadata_files


@dataclass
class Periodo:
    id: int
    literals: list[str]
    modificacao: dt.date


@dataclass
class Localidade:
    id: int
    nome: str
    id_nivel: str
    nome_nivel: str


@dataclass
class Variavel:
    id: int
    nome: str
    unidade: str


@dataclass
class Categoria:
    id: int
    nome: str
    unidade: str
    nivel: int


@dataclass
class Classificacao:
    id: int
    nome: str
    categorias: list[Categoria]


@dataclass
class Agregado:
    id: int
    nome: str
    url: str
    pesquisa: str
    assunto: str
    periodicidade_frequencia: str
    variaveis: list[Variavel]
    classificacoes: list[Classificacao]
    periodos: list[Periodo]
    localidades: list[Localidade]


def read_periodos(filepath: Path) -> list[Periodo]:
    periodos = []
    parse_modificacao = lambda x: dt.datetime.strptime(x, "%d/%m/%Y").date()
    for per in read_json(filepath):
        p = Periodo(
            id=int(per["id"]),
            literals=per["literals"],
            modificacao=parse_modificacao(per["modificacao"]),
        )
        periodos.append(p)
    return periodos


def read_localidades(filepath: Path) -> list[Localidade]:
    localidades = []
    for loc in read_json(filepath):
        l = Localidade(
            id=int(loc["id"]),
            nome=loc["nome"],
            id_nivel=loc["nivel"]["id"],
            nome_nivel=loc["nivel"]["nome"],
        )
        localidades.append(l)
    return localidades


def read_metadados(datadir: Path, pesquisa_id: str, agregado_id: int) -> Agregado:
    files = agregado_metadata_files(
        datadir=datadir,
        pesquisa_id=pesquisa_id,
        agregado_id=agregado_id,
    )
    localidades = []
    for f in files["localidades"]:
        localidades.extend(read_localidades(f))
    periodos = read_periodos(files["periodos"])
    data = read_json(files["metadados"])
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
        pesquisa=data["pesquisa"],
        assunto=data["assunto"],
        periodicidade_frequencia=data["periodicidade"]["frequencia"],
        variaveis=variaveis,
        classificacoes=classificacoes,
        periodos=periodos,
        localidades=localidades,
    )
    return a
