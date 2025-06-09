"""API de dados agregados do IBGE

Fonte: https://servicodados.ibge.gov.br/api/docs/agregados?versao=3

> Incremente sua aplicação com a API de dados agregados do IBGE, a API que
> alimenta o SIDRA, Sistema IBGE de Recuperação Automática, ferramenta que
> disponibiliza os dados das pesquisas e censos realizados pelo IBGE.
>
> A fim de aprofundar o conhecimento desta API, recomendamos que você explore
> as tabelas do SIDRA 1705 e 1712 - Cada tabela do SIDRA corresponde a um
> agregado desta API -, que são usadas como exemplos na documentação desta API.
> Se desejar, use o Query Builder para gerar consultas customizadas.
>
> obs 1: para desenvolvedores de soluções OLAP, Online Analytical Processing,
> os conceitos de variáveis, classificações e categorias são, respectivamente,
> idênticos aos de medidas, dimensões e membros.
> obs 2: a presente versão permite 3 modos de visualização das variáveis. Para
> mais informações, consulte o parâmetro view

---

In this module:

- Dataclasses for the data returned by agregados API.
- Functions to generate the URL for the IBGE's Agregados API.

"""

import datetime as dt
import urllib.parse as urlparse
from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlencode

BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"


@dataclass
class Periodo:
    id: str
    literals: list[str]
    modificacao: dt.date


@dataclass
class NivelTerritorial:
    id: str
    nome: str


@dataclass
class Localidade:
    id: str
    nome: str
    nivel: NivelTerritorial


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
    excecao: list[int]


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
class AgregadoNivelTerritorial:
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
    nivel_territorial: AgregadoNivelTerritorial
    variaveis: list[Variavel]
    classificacoes: list[Classificacao]
    periodos: list[Periodo]
    localidades: list[Localidade]


class AcervoEnum(StrEnum):
    ASSUNTO = "A"
    CLASSIFICACAO = "C"
    NIVELTERRITORIAL = "N"
    PERIODO = "P"
    PERIODICIDADE = "E"
    VARIAVEL = "V"


def build_url_agregados() -> str:
    """Obtém o conjunto de agregados, agrupados pelas respectivas pesquisas."""
    return BASE_URL


def build_url_metadados(agregado_id: int) -> str:
    """Obtém os metadados associados ao agregado."""
    return BASE_URL + f"/{agregado_id}/metadados"


def build_url_periodos(agregado_id: int) -> str:
    """Obtém os períodos associados ao agregado."""
    return BASE_URL + f"/{agregado_id}/periodos"


def build_url_localidades(agregado_id: int, localidades_nivel: str) -> str:
    """Obtém as localidades associadas ao agregado de acordo com um ou mais níveis geográficos."""
    return BASE_URL + f"/{agregado_id}/localidades/{localidades_nivel}"


def build_url_acervos(acervo: AcervoEnum) -> str:
    params = {"acervo": acervo.value}
    url_parts = list(urlparse.urlparse(BASE_URL))
    query = dict(urlparse.parse_qsl(url_parts[4]))
    query.update(params)
    url_parts[4] = urlencode(query)
    return urlparse.urlunparse(url_parts)
