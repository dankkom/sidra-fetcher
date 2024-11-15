import argparse
import queue
from pathlib import Path

import httpx

from ibge_sidra_fetcher import config, dispatcher, fetcher, storage
from ibge_sidra_fetcher.api.agregados import AcervoEnum


def get_args() -> argparse.Namespace:

    def add_common_arguments(parser: argparse.ArgumentParser):
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

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    # Command agregados
    get_agregados_parser = subparsers.add_parser("agregados")
    add_common_arguments(get_agregados_parser)
    get_agregados_parser.set_defaults(func=get_agregados)

    # Command acervos
    get_acervos_parser = subparsers.add_parser("acervos")
    add_common_arguments(get_acervos_parser)
    get_acervos_parser.set_defaults(func=get_acervos)

    # Command agregados-metadados
    get_agregados_metadados_parser = subparsers.add_parser("agregados-metadados")
    add_common_arguments(get_agregados_metadados_parser)
    get_agregados_metadados_parser.add_argument(
        "--threads",
        type=int,
        default=4,
    )
    get_agregados_metadados_parser.set_defaults(func=get_agregados_metadados)

    # Command agregados-localidades
    get_agregados_localidades_parser = subparsers.add_parser("agregados-localidades")
    add_common_arguments(get_agregados_localidades_parser)
    get_agregados_localidades_parser.add_argument(
        "--threads",
        type=int,
        default=4,
    )
    get_agregados_localidades_parser.set_defaults(func=get_agregados_localidades)

    # Command agregados-periodos
    get_agregados_periodos_parser = subparsers.add_parser("agregados-periodos")
    add_common_arguments(get_agregados_periodos_parser)
    get_agregados_periodos_parser.add_argument(
        "--threads",
        type=int,
        default=4,
    )
    get_agregados_periodos_parser.set_defaults(func=get_agregados_periodos)

    return parser.parse_args()


def get_agregados(args: argparse.Namespace):

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


def get_acervos(args: argparse.Namespace):

    with httpx.Client() as client:
        for a in AcervoEnum:
            task = dispatcher.acervo(args.data_dir, a)
            if task["dest_filepath"].exists() and not args.overwrite:
                continue
            print("Task acervo:", a)
            data = fetcher.get(task["url"], client)
            storage.write_data(data, task["dest_filepath"])
        print("Done")


def get_agregados_metadados(args: argparse.Namespace):

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
            task = dispatcher.metadados(args.data_dir, agregado_id)
            if task["dest_filepath"].exists():
                continue
            print("Task metadados:", pesquisa_id, agregado_id)
            q.put(task)
    q.join()


def get_agregados_localidades(args: argparse.Namespace):

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


def get_agregados_periodos(args: argparse.Namespace):

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
            task = dispatcher.periodos(args.data_dir, agregado_id)
            if task["dest_filepath"].exists() and not args.overwrite:
                continue
            print("Task periodos:", pesquisa_id, agregado_id)
            q.put(task)
    q.join()


def main():
    args = get_args()
    args.func(args)


if __name__ == "__main__":
    main()
