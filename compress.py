import argparse
import subprocess
from pathlib import Path


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Directory to store the fetched data",
    )
    parser.add_argument(
        "--dest-dir",
        type=Path,
        required=True,
        help="Directory to store the compressed data",
    )
    return parser.parse_args()


def compress_survey_directory(survey_directory: Path, dest_dir: Path):
    survey_code = survey_directory.name
    for aggregate_dir in survey_directory.iterdir():
        aggregate_dir_name = aggregate_dir.name
        dest_filepath = dest_dir / survey_code / f"{aggregate_dir_name}.7z"
        dest_filepath.mkdir(exist_ok=True)
        cmd = [
            "7z",
            "a",
            str(dest_filepath),
            str(aggregate_dir),
            "-mx=9",
            "-ms=4g",
        ]
        print(" ".join(cmd))
        subprocess.run(cmd)


def main():
    args = get_args()
    data_dir = args.data_dir
    dest_dir = args.dest_dir

    survey_dirs = list(data_dir.glob("*/"))
    for survey_dir in survey_dirs:
        compress_survey_directory(survey_dir, dest_dir)


if __name__ == "__main__":
    main()
