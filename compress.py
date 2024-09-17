import subprocess
from pathlib import Path


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
    data_dir = Path()
    dest_dir = Path("/run/media/dk/D0B5-F499/data/raw/ibge/sidra")
    survey_dirs = list(data_dir.glob("*/"))
    for survey_dir in survey_dirs:
        compress_survey_directory(survey_dir, dest_dir)


if __name__ == "__main__":
    main()
