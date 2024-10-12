"""Functions to generate the URL for the IBGE's Agregados API."""

import urllib.parse as urlparse
from urllib.parse import urlencode


_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"


def agregados() -> str:
    return _URL


def metadados(agregado_id: int) -> str:
    return _URL + f"/{agregado_id}/metadados"


def periodos(agregado_id: int) -> str:
    return _URL + f"/{agregado_id}/periodos"


def localidades(agregado_id: int, localidades_nivel: str) -> str:
    return _URL + f"/{agregado_id}/localidades/{localidades_nivel}"


def acervos(acervo_id: str) -> str:
    params = {"acervo": acervo_id}
    url_parts = list(urlparse.urlparse(_URL))
    query = dict(urlparse.parse_qsl(url_parts[4]))
    query.update(params)
    url_parts[4] = urlencode(query)
    return urlparse.urlunparse(url_parts)
