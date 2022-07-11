import argparse
import json
import time
from pathlib import Path

import httpx

from ibge_sidra_fetcher import api


def get_parser():
    parser = argparse.ArgumentParser(description="Download IBGE's aggregates' metadata")
    parser.add_argument("-i", "--input", default="data/sidra-agregados.json", help="Input file")
    parser.add_argument("-o", "--output", default="data", help="Output directory")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    input_file = Path(args.input)
    output_dir = Path(args.output)

    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    c = httpx.Client()

    for pesquisa in data:
        pesquisa_id = pesquisa["id"]
        pesquisa_nome = pesquisa["nome"]
        pesquisa_dir = output_dir / pesquisa_id
        pesquisa_dir.mkdir(parents=True, exist_ok=True)
        print(f"{pesquisa_id} - {pesquisa_nome}")
        agregados = pesquisa["agregados"]
        for agregado in agregados:
            agregado_id = agregado["id"]
            agregado_nome = agregado["nome"]
            output_file = pesquisa_dir / f"sidra-agregado-metadados_{agregado_id}.json"
            if output_file.exists():
                continue
            print(f"    {agregado_id: >4} - {agregado_nome}")
            try:
                agregado_metadata = api.agregados.get_agregado_metadados(agregado_id, c)
                agregado_metadata = json.loads(agregado_metadata.decode("utf-8"))
                output_file.write_text(json.dumps(agregado_metadata, indent=4))
            except Exception as e:
                print(e)
                time.sleep(5)


if __name__ == "__main__":
    main()
