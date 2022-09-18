from pathlib import Path
from typing import Any

from .storage import agregado_metadata_files, read_json


def iter_sidra_agregados(datadir: Path) -> dict[str, int | str]:
    sidra_agregados = read_json(datadir / "sidra-agregados.json")
    for pesquisa in sidra_agregados:
        pesquisa_id = pesquisa["id"]
        pesquisa_nome = pesquisa["nome"]
        for agregado in pesquisa["agregados"]:
            agregado_id = int(agregado["id"])
            agregado_nome = agregado["nome"]
            metadata_files = agregado_metadata_files(
                datadir,
                pesquisa_id,
                agregado_id,
            )
            yield {
                "pesquisa_id": pesquisa_id,
                "pesquisa_nome": pesquisa_nome,
                "agregado_id": agregado_id,
                "agregado_nome": agregado_nome,
                "metadata_files": metadata_files,
            }


def read_metadata(agregado: dict[str, Any]) -> dict:

    fm = agregado["metadata_files"]["metadados"]
    fl = agregado["metadata_files"]["localidades"]
    fp = agregado["metadata_files"]["periodos"]

    metadados = read_json(fm)
    localidades = []
    for f in fl.iterdir():
        localidades.extend(read_json(f))
    periodos = read_json(fp)

    return metadados | {"localidades": localidades, "periodos": periodos}


def calculate_aggregate(aggregate_metadata: dict) -> dict[str, dict | int]:
    localidades = aggregate_metadata["localidades"]
    variaveis = aggregate_metadata["variaveis"]
    classificacoes = aggregate_metadata["classificacoes"]
    periodos = aggregate_metadata["periodos"]

    stat_localidades = {}
    for localidade in localidades:
        if localidade["nivel"]["id"] not in stat_localidades:
            stat_localidades[localidade["nivel"]["id"]] = 0
        stat_localidades[localidade["nivel"]["id"]] += 1
    n_localidades = sum(stat_localidades.values())

    n_niveis_territoriais = len(stat_localidades)
    n_variaveis = len(variaveis)
    n_categorias = sum(
        [
            len(classificacao["categorias"])
            for classificacao in classificacoes
        ]
    )
    n_periodos = len(periodos)
    period_size = n_localidades * n_variaveis * max(n_categorias, 1)
    total_size = period_size * n_periodos
    return {
        "stat_localidades": stat_localidades,
        "n_niveis_territoriais": n_niveis_territoriais,
        "n_localidades": n_localidades,
        "n_variaveis": n_variaveis,
        "n_categorias": n_categorias,
        "n_periodos": n_periodos,
        "period_size": period_size,
        "total_size": total_size,
    }
