from functools import reduce

from .sidra.agregado import Agregado


def get_stat_localidades(agregado: Agregado) -> dict[str, int]:
    stat_localidades = {}
    for localidade in agregado.localidades:
        if localidade.id_nivel not in stat_localidades:
            stat_localidades[localidade.id_nivel] = 0
        stat_localidades[localidade.id_nivel] += 1
    return stat_localidades


def get_n_dimensoes(agregado: Agregado) -> int:
    n_dimensoes = reduce(
        lambda x, y: x * y,
        [
            len(classificacao.categorias)
            for classificacao in agregado.classificacoes
        ],
        1,
    )
    return n_dimensoes


def calculate_aggregate(agregado: Agregado) -> dict[str, dict | int]:
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
