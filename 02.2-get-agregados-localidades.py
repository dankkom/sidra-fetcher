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
            agregado_id = agregado["id"]
            agregado_metadados_filepath = storage.agregado_metadados_filepath(
                data_dir, pesquisa_id, agregado_id
            )
            if not agregado_metadados_filepath.exists():
                continue
            agregado_metadados = storage.read_json(agregado_metadados_filepath)
            for task in dispatcher.agregado_localidades(data_dir, pesquisa_id, agregado_metadados):
                if task["dest_filepath"].exists():
                    continue
                print("Task localidades:", pesquisa_id, agregado_id)
                q.put(task)
    q.join()


if __name__ == "__main__":
    main()
