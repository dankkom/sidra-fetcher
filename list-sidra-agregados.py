import argparse
import json
from ibge_sidra_fetcher import api


def get_parser():
    parser = argparse.ArgumentParser(description="List IBGE's aggregates")
    parser.add_argument("-o", "--output", default="sidra-agregados.json", help="Output file")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    output = args.output

    data = api.agregados.get_agregados()
    data = json.loads(data.decode("utf-8"))
    for pesquisa in data:
        pesquisa_id = pesquisa["id"]
        pesquisa_nome = pesquisa["nome"]
        print(f"{pesquisa_id} - {pesquisa_nome}")
        agregados = pesquisa["agregados"]
        for agregado in agregados:
            agregado_id = agregado["id"]
            agregado_nome = agregado["nome"]
            print(f"    {agregado_id: <4} - {agregado_nome}")
    with open(output, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


if __name__ == "__main__":
    main()
