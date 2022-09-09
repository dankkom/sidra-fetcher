import json
import time
from pathlib import Path

import httpx

from . import config, path, utils
from .api.agregados import (get_agregado_localidades, get_agregado_metadados,
                            get_agregado_periodos, get_agregados)


def get_agregados_periodos():

    c = httpx.Client()

    for i, agregado in enumerate(utils.iter_sidra_agregados(config.DATA_DIR)):
        pesquisa_id = agregado["pesquisa_id"]
        agregado_id = agregado["agregado_id"]
        output_file = agregado["metadata_files"]["periodos"]
        if output_file.exists():
            continue
        output_file.parent.mkdir(parents=True, exist_ok=True)
        print(f"{i: >6} {pesquisa_id} {agregado_id: >4}")
        try:
            periodos = get_agregado_periodos(agregado_id, c)
            periodos = json.loads(periodos.decode("utf-8"))
            print(f"Writing file {output_file}")
            output_file.write_text(json.dumps(periodos, indent=4))
        except Exception as e:
            print(e)
            time.sleep(5)


def list_sidra_agregados():

    output = config.DATA_DIR / "sidra-agregados.json"

    if output.exists():
        with open(output, "r") as f:
            data = json.load(f)
        for pesquisa in data:
            n_agregados = len(pesquisa["agregados"])
            print(f"{pesquisa['id']} - {pesquisa['nome']: <70}{n_agregados: >4}")
        return

    data = get_agregados()
    data = json.loads(data.decode("utf-8"))
    for pesquisa in data:
        pesquisa_id = pesquisa["id"]
        pesquisa_nome = pesquisa["nome"]
        print(f"{pesquisa_id} - {pesquisa_nome}")
        agregados = pesquisa["agregados"]
        for agregado in agregados:
            agregado_id = int(agregado["id"])
            agregado_nome = agregado["nome"]
            print(f"    {agregado_id: <4} - {agregado_nome}")
    with open(output, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def get_agregados_metadados():

    c = httpx.Client()

    for agregado in utils.iter_sidra_agregados(config.DATA_DIR):
        agregado_id = agregado["agregado_id"]
        agregado_nome = agregado["agregado_nome"]
        output_file = agregado["metadata_files"]["metadados"]
        if output_file.exists():
            continue
        output_file.parent.mkdir(parents=True, exist_ok=True)
        print(f"    {agregado_id: >4} - {agregado_nome}")
        try:
            agregado_metadata = get_agregado_metadados(
                agregado_id,
                c,
            )
            agregado_metadata = json.loads(agregado_metadata.decode("utf-8"))
            print(f"Writing file {output_file}")
            output_file.write_text(json.dumps(agregado_metadata, indent=4))
        except Exception as e:
            print(e)
            time.sleep(5)


def get_agregados_localidades():

    def get_niveis(metadados):
        administrativo = metadados["nivelTerritorial"]["Administrativo"]
        especial = metadados["nivelTerritorial"]["Especial"]
        ibge = metadados["nivelTerritorial"]["IBGE"]
        return set(administrativo + especial + ibge)

    c = httpx.Client()

    for agregado in utils.iter_sidra_agregados(config.DATA_DIR):
        pesquisa_id = agregado["pesquisa_id"]
        agregado_id = agregado["agregado_id"]
        agregado_nome = agregado["agregado_nome"]
        metadata_path = agregado["metadata_files"]
        metadados_filepath = metadata_path["metadados"]
        metadados = json.load(metadados_filepath.open("r", encoding="utf-8"))
        niveis = get_niveis(metadados)
        localidades_dir: Path = metadata_path["localidades"]
        localidades_dir.mkdir(parents=True, exist_ok=True)
        print(f"    {agregado_id: >4} - {agregado_nome}")
        for nivel in niveis:
            output_file = path.agregado_metadata_localidades_nivel_file(
                datadir=config.DATA_DIR,
                pesquisa_id=pesquisa_id,
                agregado_id=agregado_id,
                localidades_nivel=nivel,
            )
            if output_file.exists():
                continue
            try:
                agregado_localidades = get_agregado_localidades(
                    agregado_id,
                    nivel,
                    c,
                )
                print(f"Writing file {output_file}")
                output_file.write_text(json.dumps(agregado_localidades, indent=4))
            except Exception as e:
                print(e)
                time.sleep(5)


def calculate_data_per_agregado():

    def calculate(metadata):
        n_niveis_territoriais = len(
            set(loc["nivel"]["id"] for loc in metadata["localidades"])
        )
        n_localidades = len(metadata["localidades"])
        n_variaveis = len(metadata["variaveis"])
        n_categorias = sum(
            [
                len(classificacao["categorias"])
                for classificacao in metadata["classificacoes"]
            ]
        )
        n_periodos = len(metadata["periodos"])
        period_size = n_localidades * n_variaveis * max(n_categorias, 1)
        total_size = period_size * n_periodos
        return {
            "n_niveis_territoriais": n_niveis_territoriais,
            "n_localidades": n_localidades,
            "n_variaveis": n_variaveis,
            "n_categorias": n_categorias,
            "n_periodos": n_periodos,
            "period_size": period_size,
            "total_size": total_size,
        }

    for agregado in utils.iter_sidra_agregados(config.DATA_DIR):
        pesquisa_id = agregado["pesquisa_id"]
        agregado_id = agregado["agregado_id"]
        metadata = utils.read_metadata(agregado)
        n = calculate(metadata)
        yield n | {
            "pesquisa_id": pesquisa_id,
            "agregado_id": agregado_id,
        }
