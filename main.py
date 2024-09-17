import argparse

import requests

from ibge_sidra_fetcher import api, config, fetcher, storage


def get_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    agregados_parser = subparsers.add_parser("agregados")
    agregados_parser.set_defaults(func=do_fetch_agregados)
    metadados_parser = subparsers.add_parser("metadados")
    metadados_parser.set_defaults(func=do_fetch_metadados)
    periodos_parser = subparsers.add_parser("periodos")
    periodos_parser.set_defaults(func=do_fetch_periodos)
    localidades_parser = subparsers.add_parser("localidades")
    localidades_parser.set_defaults(func=do_fetch_localidades)
    return parser.parse_args()


def main():
    args = get_args()
    c = requests.Session()
    fetcher.get_sidra_agregados(c)
    fetcher.get_agregados_metadados(c)
    fetcher.get_agregado_periodos(c)
    fetcher.get_agregados_localidades(c)


if __name__ == "__main__":
    main()
