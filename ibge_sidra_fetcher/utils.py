from pathlib import Path
from typing import Generator

from .api.agregados.agregado import Agregado, Localidade, Periodo, Variavel
from .api.sidra.parametro import Parametro
from .stats import calculate_aggregate
from . import storage

SIZE_THRESHOLD = 50_000


def iter_sidra_agregados(datadir: Path) -> Generator[Agregado, None, None]:
    sidra_agregados = storage.read_json(datadir / "sidra-agregados.json")
    for pesquisa in sidra_agregados:
        pesquisa_id = pesquisa["id"]
        for agregado in pesquisa["agregados"]:
            agregado_id = agregado["id"]
            yield storage.read_metadados(
                datadir=datadir,
                pesquisa_id=pesquisa_id,
                agregado_id=agregado_id,
            )


def get_territories_all(agregado: Agregado) -> dict[str, list[str]]:
    localidade_nivel = sorted(
        set(loc.id_nivel.strip("N") for loc in agregado.localidades)
    )
    return {loc_nivel_id: ["all"] for loc_nivel_id in localidade_nivel}


def get_parameter_territories_all(agregado: Agregado, periodo: Periodo) -> Parametro:
    return Parametro(
        aggregate=str(agregado.id),
        territories=get_territories_all(agregado),
        variables=["all"],
        periods=[str(periodo.id)],
        classifications={c.id: ["all"] for c in agregado.classificacoes},
    )


def get_parameter_localidade(
    agregado: Agregado, periodo: Periodo, localidade: Localidade
) -> Parametro:
    return Parametro(
        aggregate=str(agregado.id),
        territories={
            localidade.id_nivel.strip("N"): [str(localidade.id)],
        },
        variables=["all"],
        periods=[str(periodo.id)],
        classifications={c.id: ["all"] for c in agregado.classificacoes},
    )


def get_parameter_localidade_variavel(
    agregado: Agregado,
    periodo: Periodo,
    localidade: Localidade,
    variavel: Variavel,
) -> Parametro:
    return Parametro(
        aggregate=str(agregado.id),
        territories={
            localidade.id_nivel.strip("N"): [str(localidade.id)],
        },
        variables=[variavel.id],
        periods=[str(periodo.id)],
        classifications={c.id: ["all"] for c in agregado.classificacoes},
    )


def iter_tasks_agregado_periodo_localidade(
    datadir: Path,
    agregado: Agregado,
    periodo: Periodo,
    localidade: Localidade,
) -> Generator[tuple[str, Path], None, None]:
    for variavel in agregado.variaveis:
        parameter = get_parameter_localidade_variavel(
            agregado=agregado,
            periodo=periodo,
            localidade=localidade,
            variavel=variavel,
        )
        dest_filepath = storage.data_filepath(
            datadir=datadir,
            pesquisa_id=agregado.pesquisa.id,
            agregado_id=agregado.id,
            periodo_id=periodo.id,
            localidade_id=localidade.id,
            variavel_id=variavel.id,
        )
        yield parameter.url(), dest_filepath


def iter_tasks_agregado_periodo(
    datadir: Path, agregado: Agregado, periodo: Periodo
) -> Generator[tuple[str, Path], None, None]:
    for localidade in agregado.localidades:
        parameter = get_parameter_localidade(agregado=agregado, periodo=periodo)
        dest_filepath = storage.data_filepath(
            datadir=datadir,
            pesquisa_id=agregado.pesquisa.id,
            agregado_id=agregado.id,
            periodo_id=periodo.id,
            localidade_id=localidade.id,
        )
        yield parameter.url(), dest_filepath


def iter_tasks_agregado(
    datadir: Path,
    agregado: Agregado,
) -> Generator[tuple[str, Path], None, None]:
    for periodo in agregado.periodos:
        dest_filepath = storage.data_filepath(
            datadir=datadir,
            pesquisa_id=agregado.pesquisa.id,
            agregado=agregado,
            periodo=periodo,
        )
        parameter = get_parameter_territories_all(agregado, periodo)
        yield parameter.url(), dest_filepath


def iter_tasks(datadir) -> Generator[tuple[str, Path], None, None]:
    for agregado in iter_sidra_agregados(datadir):
        m = calculate_aggregate(agregado)
        periodo_size = m["n_dimensoes"] * m["n_variaveis"] * m["n_localidades"]
        periodo_localidade_size = m["n_dimensoes"] * m["n_variaveis"]
        periodo_localidade_variavel_size = m["n_dimensoes"]
        if periodo_size <= SIZE_THRESHOLD:
            yield from iter_tasks_agregado(datadir=datadir, agregado=agregado)
        elif periodo_localidade_size < SIZE_THRESHOLD:
            for periodo in agregado.periodos:
                yield from iter_tasks_agregado_periodo(
                    datadir=datadir,
                    agregado=agregado,
                    periodo=periodo,
                )
        elif periodo_localidade_variavel_size < SIZE_THRESHOLD:
            for periodo in agregado.periodos:
                for localidade in agregado.localidades:
                    yield from iter_tasks_agregado_periodo_localidade(
                        datadir=datadir,
                        agregado=agregado,
                        periodo=periodo,
                        localidade=localidade,
                    )
        else:
            print("Too large!")


def unnest_classificacoes(
    classificacoes: list[dict],
    data: dict[str, str] = None,
) -> Generator[dict[str, str], None, None]:
    """Recursively list all classifications and categories"""
    if data is None:
        data = {}
    for i, classificacao in enumerate(classificacoes, 1):
        classificacao_id = classificacao["id"]
        for categoria in classificacao["categorias"]:
            categoria_id = str(categoria["id"])
            if categoria_id == "0":
                continue
            data[f"{classificacao_id}"] = categoria_id
            if len(classificacoes) == 1:
                yield dict(**data)
            else:
                yield from unnest_classificacoes(classificacoes[i:], data)
