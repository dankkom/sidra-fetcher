# Copyright (C) 2020-2026 Daniel Kiyoyudi Komesu
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <https://www.gnu.org/licenses/>.


"""Utilities to compute statistics and size estimates for agregados.

This module provides small helper functions that operate on an
`:class:`Agregado` object (from ``sidra_fetcher.api.agregados``) and
return useful summaries used for planning downloads or analysing the
data shape. Available functions include:

- ``get_stat_localidades``: counts localidades per territorial level.
- ``get_n_dimensoes``: computes the product of category counts across
    classifications (i.e. total number of dimension combinations).
- ``calculate_aggregate``: returns a dictionary with several metrics
    and size estimates (period/locality/variable dimensions and totals).
"""

from functools import reduce
from typing import Any

from .agregados import Agregado


def get_stat_localidades(agregado: Agregado) -> dict[str, int]:
    """Count localities per territorial level for an aggregate.

    Args:
        agregado: An :class:`Agregado` object containing `localidades`.

    Returns:
        A mapping from territorial level id to the number of localidades.
    """
    stat_localidades: dict[str, int] = {}
    for localidade in agregado.localidades:
        nivel_id = localidade.nivel.id
        if nivel_id not in stat_localidades:
            stat_localidades[nivel_id] = 0
        stat_localidades[nivel_id] += 1
    return stat_localidades


def get_n_dimensoes(agregado: Agregado) -> int:
    """Compute the product of category counts across classifications.

    Args:
        agregado: Aggregate whose classifications are used.

    Returns:
        The total number of unique dimension combinations (product of
        category counts). Returns 1 when there are no classifications.
    """
    n_dimensoes = reduce(
        lambda x, y: x * y,
        [
            len(classificacao.categorias)
            for classificacao in agregado.classificacoes
        ],
        1,
    )
    return n_dimensoes


def calculate_aggregate(agregado: Agregado) -> dict[str, Any]:
    """Calculate size and basic statistics for an aggregate.

    Args:
        agregado: Aggregate metadata object.

    Returns:
        A dictionary with counts (localidades, variaveis, classificacoes),
        dimension and period sizes and estimated total result size.
    """
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
