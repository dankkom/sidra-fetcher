import argparse
from pathlib import Path

import httpx

from ibge_sidra_fetcher import dispatcher, fetcher, storage
from ibge_sidra_fetcher.api.agregados.agregado import Acervo


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

    with httpx.Client() as client:
        for a in Acervo:
            task = dispatcher.acervo(args.data_dir, a)
            if task["dest_filepath"].exists() and not args.overwrite:
                continue
            print("Task acervo:", a)
            data = fetcher.get(task["url"], client)
            storage.write_data(data, task["dest_filepath"])
        print("Done")


if __name__ == "__main__":
    main()
