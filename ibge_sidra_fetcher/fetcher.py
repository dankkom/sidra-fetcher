import time

import httpx

from . import utils
from .api import agregados
from .config import DATA_DIR
from .storage import (agregado_metadata_localidades_nivel_filepath, read_json,
                      write_json)


def get_sidra_agregados(c: httpx.Client):
    output = DATA_DIR / "sidra-agregados.json"
    data = agregados.get_agregados(c)
    with open(output, "wb") as f:
        f.write(data)


def get_agregados_periodos(c: httpx.Client):
    for agregado in utils.iter_sidra_agregados(DATA_DIR):
        agregado_id = agregado["agregado_id"]
        output_file = agregado["metadata_files"]["periodos"]
        if output_file.exists():
            continue
        try:
            periodos = agregados.get_periodos(agregado_id, c)
            write_json(data=periodos, filepath=output_file)
        except Exception as e:
            print(e)
            time.sleep(5)


def get_agregados_metadados(c: httpx.Client):
    for agregado in utils.iter_sidra_agregados(DATA_DIR):
        agregado_id = agregado["agregado_id"]
        output_file = agregado["metadata_files"]["metadados"]
        if output_file.exists():
            continue
        try:
            agregado_metadata = agregados.get_metadados(agregado_id, c)
            write_json(data=agregado_metadata, filepath=output_file)
        except Exception as e:
            print(e)
            time.sleep(5)


def get_agregados_localidades(c: httpx.Client):

    def get_niveis(metadados):
        nivel_territorial = metadados["nivelTerritorial"]
        administrativo = nivel_territorial["Administrativo"]
        especial = nivel_territorial["Especial"]
        ibge = nivel_territorial["IBGE"]
        return set(administrativo + especial + ibge)

    for agregado in utils.iter_sidra_agregados(DATA_DIR):
        pesquisa_id = agregado["pesquisa_id"]
        agregado_id = agregado["agregado_id"]
        metadata_path = agregado["metadata_files"]
        metadados_filepath = metadata_path["metadados"]
        metadados = read_json(metadados_filepath)
        niveis = get_niveis(metadados)
        for nivel in niveis:
            output_file = agregado_metadata_localidades_nivel_filepath(
                datadir=DATA_DIR,
                pesquisa_id=pesquisa_id,
                agregado_id=agregado_id,
                localidades_nivel=nivel,
            )
            if output_file.exists():
                continue
            try:
                agregado_localidades = agregados.get_localidades(
                    agregado_id,
                    nivel,
                    c,
                )
                write_json(data=agregado_localidades, filepath=output_file)
            except Exception as e:
                print(e)
                time.sleep(5)
