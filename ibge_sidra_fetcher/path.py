from pathlib import Path


def pesquisa_dir(datadir: Path, pesquisa_id: str) -> Path:
    return datadir / pesquisa_id.lower()


def agregado_dir(datadir: Path, pesquisa_id: str, agregado_id: int) -> Path:
    return pesquisa_dir(datadir, pesquisa_id) / f"agregado-{agregado_id:05}"


def agregado_metadata_dir(datadir: Path, pesquisa_id: str, agregado_id: int) -> Path:
    return agregado_dir(datadir, pesquisa_id, agregado_id) / "metadata"


def agregado_metadata_files(datadir: Path, pesquisa_id: str, agregado_id: int) -> Path:
    metadata_dir = agregado_metadata_dir(datadir, pesquisa_id, agregado_id)
    return {
        "metadados": metadata_dir / "metadados.json",
        "localidades": metadata_dir / "localidades",
        "periodos": metadata_dir / "periodos.json",
    }


def agregado_metadata_localidades_nivel_file(
    datadir: Path,
    pesquisa_id: str,
    agregado_id: int,
    localidades_nivel: str,
) -> Path:
    metadata_dir = agregado_metadata_dir(datadir, pesquisa_id, agregado_id)
    return metadata_dir / "localidades" / f"{localidades_nivel.lower()}.json"
