import argparse
import json
import time
from pathlib import Path

import httpx

from ibge_sidra_fetcher import api, utils


def get_parser():
    parser = argparse.ArgumentParser(description="Get IBGE's agregados' periodos")
    parser.add_argument("-datadir", "--datadir", default="data", dest="datadir", help="Data directory")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    datadir = Path(args.datadir)

    utils.load_logging_config()

    c = httpx.Client()

    for agregado in utils.iter_sidra_agregados(datadir):
        agregado_id = agregado["agregado_id"]
        agregado_nome = agregado["agregado_nome"]
        output_file = agregado["metadata_files"]["periodos"]
        if output_file.exists():
            continue
        output_file.parent.mkdir(parents=True, exist_ok=True)
        print(f"    {agregado_id: >4} - {agregado_nome}")
        try:
            periodos = api.agregados.get_agregado_periodos(agregado_id, c)
            periodos = json.loads(periodos.decode("utf-8"))
            print(f"Writing file {output_file}")
            output_file.write_text(json.dumps(periodos, indent=4))
        except Exception as e:
            print(e)
            time.sleep(5)


if __name__ == "__main__":
    main()
