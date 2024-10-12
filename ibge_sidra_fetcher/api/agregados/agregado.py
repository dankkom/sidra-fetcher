"""Dataclasses for the data returned by agregados API."""

import datetime as dt
from dataclasses import dataclass
from enum import StrEnum


@dataclass
class Periodo:
    id: str
    literals: list[str]
    modificacao: dt.date


@dataclass
class LocalidadeNivel:
    id: str
    nome: str


@dataclass
class Localidade:
    id: str
    nome: str
    nivel: LocalidadeNivel


@dataclass
class Variavel:
    id: int
    nome: str
    unidade: str
    sumarizacao: list[str]


@dataclass
class Categoria:
    id: int
    nome: str
    unidade: str | None
    nivel: int


@dataclass
class ClassificacaoSumarizacao:
    status: bool
    excecao: list


@dataclass
class Classificacao:
    id: int
    nome: str
    sumarizacao: ClassificacaoSumarizacao
    categorias: list[Categoria]


@dataclass
class Pesquisa:
    id: str
    nome: str


@dataclass
class Periodicidade:
    frequencia: str
    inicio: str
    fim: str


@dataclass
class NivelTerritorial:
    administrativo: list[str]
    especial: list[str]
    ibge: list[str]


@dataclass
class Agregado:
    id: int
    nome: str
    url: str
    pesquisa: Pesquisa
    assunto: str
    periodicidade: Periodicidade
    nivel_territorial: NivelTerritorial
    variaveis: list[Variavel]
    classificacoes: list[Classificacao]
    periodos: list[Periodo]
    localidades: list[Localidade]


class Acervo(StrEnum):
    ASSUNTO = "A"
    CLASSIFICACAO = "C"
    NIVELTERRITORIAL = "N"
    PERIODO = "P"
    PERIODICIDADE = "E"
    VARIAVEL = "V"
