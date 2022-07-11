import argparse
from pathlib import Path


def get_parser():
    parser = argparse.ArgumentParser(description="Fetch metadata from IBGE's aggregates APIs")
    parser.add_argument("--output", type=Path, help="Output directory")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()


if __name__ == "__main__":
    main()
