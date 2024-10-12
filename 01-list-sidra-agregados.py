import argparse
from pathlib import Path

import httpx

from ibge_sidra_fetcher import api, config, storage


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Directory to store the fetched data",
    )
    return parser.parse_args()


def main():
    args = get_args()
    data_dir = args.data_dir

    # sidra_agregados_filepath = storage.sidra_agregados_filepath(data_dir=config.DATA_DIR)
    sidra_agregados_filepath = storage.sidra_agregados_filepath(data_dir=data_dir)

    # Fetch and store the list of surveys if it doesn't exist
    if not sidra_agregados_filepath.exists():
        client = httpx.Client(timeout=config.TIMEOUT)
        sidra_agregados_data = api.agregados.handler.get_agregados(client=client)
        storage.write_data(sidra_agregados_data, sidra_agregados_filepath)
        client.close()

    # Read the list of surveys
    sidra_agregados_data = storage.read_json(sidra_agregados_filepath)

    counts = {}

    # Fetch and store the metadata of each agregado
    for pesquisa in sidra_agregados_data:
        pesquisa_id = pesquisa["id"]
        counts[pesquisa_id] = 0
        for agregado in pesquisa["agregados"]:
            agregado_id = int(agregado["id"])
            agregado_nome = agregado["nome"]
            print(pesquisa_id, agregado_id, agregado_nome)
            counts[pesquisa_id] += 1

    for pesquisa_id, count in counts.items():
        print(pesquisa_id, count)

    print(f"Total: {sum(counts.values())}")


if __name__ == "__main__":
    main()
