import datetime as dt
from dataclasses import dataclass


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
class Pesquisa:
    id: str
    nome: str


@dataclass
class Agregado:
    id: int
    nome: str
    url: str
    pesquisa: Pesquisa
    assunto: str
    periodicidade_frequencia: str
    variaveis: list[Variavel]
    classificacoes: list[Classificacao]
    periodos: list[Periodo]
    localidades: list[Localidade]
