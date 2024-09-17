"""Functions to generate the URL for the IBGE's Agregados API."""

_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"


def agregados() -> str:
    return _URL


def metadados(agregado_id: int) -> str:
    return _URL + f"/{agregado_id}/metadados"


def periodos(agregado_id: int) -> str:
    return _URL + f"/{agregado_id}/periodos"


def localidades(agregado_id: int, localidades_nivel: str) -> str:
    return _URL + f"/{agregado_id}/localidades/{localidades_nivel}"
