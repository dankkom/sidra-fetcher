"""Módulo com funções para calcular estatísticas sobre os metadados dos agregados."""

from functools import reduce

from .api.agregados import Agregado


def get_stat_localidades(agregado: Agregado) -> dict[str, int]:
    """Calcula a quantidade de localidades por nível territorial."""
    stat_localidades = {}
    for localidade in agregado.localidades:
        nivel_id = localidade.nivel.id
        if nivel_id not in stat_localidades:
            stat_localidades[nivel_id] = 0
        stat_localidades[nivel_id] += 1
    return stat_localidades


def get_n_dimensoes(agregado: Agregado) -> int:
    """Calcula o número de dimensões do agregado."""
    n_dimensoes = reduce(
        lambda x, y: x * y,
        [len(classificacao.categorias) for classificacao in agregado.classificacoes],
        1,
    )
    return n_dimensoes


def calculate_aggregate(agregado: Agregado) -> dict[str, dict | int]:
    """Calcula estatísticas sobre o agregado."""
    stat_localidades = get_stat_localidades(agregado)
    n_localidades = sum(stat_localidades.values())

    n_niveis_territoriais = len(stat_localidades)
    n_variaveis = len(agregado.variaveis)
    n_classificacoes = len(agregado.classificacoes)
    n_dimensoes = get_n_dimensoes(agregado=agregado)
    n_periodos = len(agregado.periodos)
    period_size = n_localidades * n_variaveis * max(n_dimensoes, 1)
    total_size = period_size * n_periodos
    localidade_size = max(n_variaveis, 1) * max(n_dimensoes, 1)
    variavel_size = max(n_dimensoes, 1)
    return {
        "pesquisa_id": agregado.pesquisa.id,
        "agregado_id": agregado.id,
        "stat_localidades": stat_localidades,
        "n_niveis_territoriais": n_niveis_territoriais,
        "n_localidades": n_localidades,
        "n_variaveis": n_variaveis,
        "n_classificacoes": n_classificacoes,
        "n_dimensoes": n_dimensoes,
        "n_periodos": n_periodos,
        "period_size": period_size,
        "localidade_size": localidade_size,
        "variavel_size": variavel_size,
        "total_size": total_size,
    }
