import subprocess
from pathlib import Path


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
    data_dir = Path("F:/data/raw/ibge-sidra")
    survey_7z_dir = Path("I:/backup/ibge-sidra")
    survey_7z_files = list(survey_7z_dir.glob("*.7z"))
    for survey_7z in survey_7z_files:
        decompress_survey_7z(survey_7z, data_dir)


if __name__ == "__main__":
    main()
