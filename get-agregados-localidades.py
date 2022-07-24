import argparse
import json
import time
from pathlib import Path

import httpx

from ibge_sidra_fetcher import api, utils


def get_parser():
    parser = argparse.ArgumentParser(description="Download IBGE's agregados' localidades")
    parser.add_argument("-datadir", "--datadir", default="data", help="Data directory")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    datadir = Path(args.datadir)

    utils.load_logging_config()

    def get_niveis(metadados):
        administrativo = metadados["nivelTerritorial"]["Administrativo"]
        especial = metadados["nivelTerritorial"]["Especial"]
        ibge = metadados["nivelTerritorial"]["IBGE"]
        return administrativo + especial + ibge

    c = httpx.Client()

    for agregado in utils.iter_sidra_agregados(datadir):
        agregado_id = agregado["agregado_id"]
        agregado_nome = agregado["agregado_nome"]
        metadata_path = agregado["metadata_files"]
        metadados_filepath = metadata_path["metadados"]
        metadados = json.load(metadados_filepath.open("r", encoding="utf-8"))
        niveis = get_niveis(metadados)
        output_file = metadata_path["localidades"]
        if output_file.exists():
            continue
        print(f"    {agregado_id: >4} - {agregado_nome}")
        try:
            agregado_localidades = api.agregados.get_agregado_localidades(agregado_id, niveis, c)
            print(f"Writing file {output_file}")
            output_file.write_text(json.dumps(agregado_localidades, indent=4))
        except Exception as e:
            print(e)
            time.sleep(5)


if __name__ == "__main__":
    main()
