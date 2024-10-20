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
    parser.add_argument(
        "--overwrite",
        action="store_true",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
    )
    return parser.parse_args()


def main():
    args = get_args()

    agregados_filepath = storage.agregados_filepath(data_dir=args.data_dir)
    agregados_data = storage.read_json(agregados_filepath)

    q = queue.Queue()
    for _ in range(args.threads):
        fetcher_worker = fetcher.Fetcher(q)
        fetcher_worker.start()

    for pesquisa in agregados_data:
        pesquisa_id = pesquisa["id"]
        for agregado in pesquisa["agregados"]:
            agregado_id = int(agregado["id"])
            agregado_metadados_filepath = storage.agregado_metadados_filepath(
                args.data_dir, agregado_id
            )
            if not agregado_metadados_filepath.exists():
                continue
            agregado_metadados = storage.read_json(agregado_metadados_filepath)
            tasks = dispatcher.agregado_localidades(args.data_dir, agregado_metadados)
            for task in tasks:
                if task["dest_filepath"].exists() and not args.overwrite:
                    continue
                print(f"Task localidades: {pesquisa_id}/{agregado_id:05}")
                q.put(task)
    q.join()


if __name__ == "__main__":
    main()
