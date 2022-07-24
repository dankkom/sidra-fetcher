import argparse
import json
from pathlib import Path

from ibge_sidra_fetcher import api


def get_parser():
    parser = argparse.ArgumentParser(description="List IBGE's aggregates")
    parser.add_argument("-datadir", "--datadir", default="data", dest="datadir", help="Data directory")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    datadir = Path(args.datadir)
    datadir.mkdir(parents=True, exist_ok=True)
    output = datadir / "sidra-agregados.json"

    if output.exists():
        with open(output, "r") as f:
            data = json.load(f)
        for pesquisa in data:
            n_agregados = len(pesquisa["agregados"])
            print(f"{pesquisa['id']} - {pesquisa['nome']: <70}{n_agregados: >4}")
        return

    data = api.agregados.get_agregados()
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


if __name__ == "__main__":
    main()
