import logging.config
import json
import os
from pathlib import Path

from .path import agregado_metadata_files


def load_logging_config():
    CONFIG_DIR = Path(os.getenv("CONFIG_DIR"))
    if not CONFIG_DIR:
        raise FileNotFoundError("No logging configuration file exists")
    logging_config_filepath = CONFIG_DIR / "ibge-sidra-fetcher" / "logging.ini"
    if not logging_config_filepath.exists():
        raise FileNotFoundError("No logging configuration file exists")
    logging.config.fileConfig(logging_config_filepath)


def iter_sidra_agregados(datadir):
    with open(datadir / "sidra-agregados.json", "r", encoding="utf-8") as f:
        sidra_agregados = json.load(f)
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
