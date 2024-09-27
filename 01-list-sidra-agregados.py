import argparse
import queue
from pathlib import Path

import httpx

from ibge_sidra_fetcher import config, dispatcher, fetcher, storage


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

    client = httpx.Client(timeout=config.TIMEOUT)

    # sidra_agregados_filepath = storage.sidra_agregados_filepath(datadir=config.DATA_DIR)
    sidra_agregados_filepath = storage.sidra_agregados_filepath(data_dir=data_dir)

    # Fetch and store the list of surveys if it doesn't exist
    if not sidra_agregados_filepath.exists():
        sidra_agregados_data = fetcher.sidra_agregados(client=client)
        storage.write_data(sidra_agregados_data, sidra_agregados_filepath)

    # Read the list of surveys
    sidra_agregados_data = storage.read_json(sidra_agregados_filepath)

    q = queue.Queue()
    for _ in range(4):
        worker = fetcher.Fetcher(q=q)
        worker.start()

    # Fetch and store the metadata of each agregado
    for pesquisa in sidra_agregados_data:
        pesquisa_id = pesquisa["id"]
        for agregado in pesquisa["agregados"]:
            agregado_id = agregado["id"]
            task = dispatcher.metadados(data_dir, pesquisa_id, agregado_id)
            q.put(task)

    q.join()


if __name__ == "__main__":
    main()
