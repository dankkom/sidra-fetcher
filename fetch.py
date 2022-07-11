import argparse
from pathlib import Path

from ibge_sidra_fetcher import sidra


def get_parser():
    parser = argparse.ArgumentParser(description="Fetch data from IBGE's aggregates APIs")
    parser.add_argument("--datadir", type=Path, help="Data directory")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()


if __name__ == "__main__":
    main()
