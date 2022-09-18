import datetime as dt
import json
from pathlib import Path


def read_json(filepath: Path):
    with filepath.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def write_data(data: bytes, filepath: Path):
    print(f"Writing bytes file {filepath}")
    filepath.parent.mkdir(exist_ok=True, parents=True)
    with filepath.open("wb") as f:
        f.write(data)


def write_json(data: dict | list | bytes, filepath: Path):
    print(f"Writing JSON file {filepath}")
    if isinstance(data, bytes):
        data = json.loads(data.decode("utf-8"))
    filepath.parent.mkdir(exist_ok=True, parents=True)
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def pesquisa_dir(datadir: Path, pesquisa_id: str) -> Path:
    return datadir / pesquisa_id.lower()


def agregado_dir(datadir: Path, pesquisa_id: str, agregado_id: int) -> Path:
    return pesquisa_dir(datadir, pesquisa_id) / f"agregado-{agregado_id:05}"


def agregado_metadata_dir(
    datadir: Path,
    pesquisa_id: str,
    agregado_id: int,
) -> Path:
    return agregado_dir(datadir, pesquisa_id, agregado_id) / "metadata"


def get_filepath(
    datadir: Path,
    pesquisa_id: str,
    agregado_id: int,
    periodo_id: int,
    modificacao: dt.date,
) -> Path:
    d = agregado_dir(datadir, pesquisa_id, agregado_id)
    filename = f"{agregado_id}_{periodo_id}_{modificacao:%Y%m%d}.json"
    return d / filename


def agregado_metadata_files(
    datadir: Path,
    pesquisa_id: str,
    agregado_id: int,
) -> dict[str, Path | list[Path]]:
    metadata_dir = agregado_metadata_dir(datadir, pesquisa_id, agregado_id)
    return {
        "metadados": metadata_dir / "metadados.json",
        "localidades": list((metadata_dir / "localidades").iterdir()),
        "periodos": metadata_dir / "periodos.json",
    }


def agregado_metadata_localidades_nivel_filepath(
    datadir: Path,
    pesquisa_id: str,
    agregado_id: int,
    localidades_nivel: str,
) -> Path:
    metadata_dir = agregado_metadata_dir(datadir, pesquisa_id, agregado_id)
    return metadata_dir / "localidades" / f"{localidades_nivel.lower()}.json"
