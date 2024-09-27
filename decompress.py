import argparse
import subprocess
from pathlib import Path


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--survey-7z",
        type=Path,
        required=True,
        help="Path to the 7z file",
    )
    parser.add_argument(
        "--dest-dir",
        type=Path,
        required=True,
        help="Directory to store the decompressed data",
    )
    return parser.parse_args()


def decompress_survey_7z(survey_7z: Path, dest_dir: Path):
    dest_dir.mkdir(exist_ok=True, parents=True)
    cmd = [
        "7z",
        "x",
        str(survey_7z),
        f"-o{dest_dir}",
    ]
    print(" ".join(cmd))
    subprocess.run(cmd)


def main():
    args = get_args()
    dest_dir = args.dest_dir
    survey_7z_dir = args.survey_7z

    survey_7z_files = list(survey_7z_dir.glob("*.7z"))
    for survey_7z in survey_7z_files:
        decompress_survey_7z(survey_7z, dest_dir)


if __name__ == "__main__":
    main()
