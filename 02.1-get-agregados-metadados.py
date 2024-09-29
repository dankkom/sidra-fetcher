import argparse
import queue
from pathlib import Path

from ibge_sidra_fetcher import dispatcher, fetcher, storage


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

    sidra_agregados_filepath = storage.sidra_agregados_filepath(data_dir=data_dir)
    sidra_agregados_data = storage.read_json(sidra_agregados_filepath)

    q = queue.Queue()
    for _ in range(4):
        fetcher_worker = fetcher.Fetcher(q)
        fetcher_worker.start()

    for pesquisa in sidra_agregados_data:
        pesquisa_id = pesquisa["id"]
        for agregado in pesquisa["agregados"]:
            agregado_id = int(agregado["id"])
            task = dispatcher.metadados(data_dir, pesquisa_id, agregado_id)
            if task["dest_filepath"].exists():
                continue
            print("Task metadados:", pesquisa_id, agregado_id)
            q.put(task)
    q.join()


if __name__ == "__main__":
    main()
