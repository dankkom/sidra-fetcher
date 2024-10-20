import argparse
from pathlib import Path

import httpx

from ibge_sidra_fetcher import fetcher, config, storage


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Directory to store the fetched data",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
    )
    return parser.parse_args()


def main():
    args = get_args()

    agregados_filepath = storage.agregados_filepath(data_dir=args.data_dir)

    # Fetch and store the list of surveys if it doesn't exist
    if not agregados_filepath.exists() and not args.overwrite:
        client = httpx.Client(timeout=config.TIMEOUT, verify=False)
        print("Fetching the list of surveys")
        print(agregados_filepath)
        agregados_data = fetcher.get_agregados(client=client)
        storage.write_data(agregados_data, agregados_filepath)
        client.close()

    # Read the list of surveys
    agregados_data = storage.read_json(agregados_filepath)

    counts = {}

    # Fetch and store the metadata of each agregado
    for pesquisa in agregados_data:
        pesquisa_id = pesquisa["id"]
        counts[pesquisa_id] = len(pesquisa["agregados"])

    for pesquisa_id, count in counts.items():
        print(pesquisa_id, count)

    print(f"Total: {sum(counts.values())}")


if __name__ == "__main__":
    main()
