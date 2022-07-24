from pathlib import Path


def pesquisa_dir(datadir: Path, pesquisa_id: str) -> Path:
    return datadir / pesquisa_id.lower()


def agregado_dir(datadir: Path, pesquisa_id: str, agregado_id: int) -> Path:
    return pesquisa_dir(datadir, pesquisa_id) / f"agregado-{agregado_id:05}"


def agregado_metadata_dir(datadir: Path, pesquisa_id: str, agregado_id: int) -> Path:
    return agregado_dir(datadir, pesquisa_id, agregado_id) / "metadata"


def agregado_metadata_files(datadir: Path, pesquisa_id: str, agregado_id: int) -> Path:
    metadata_dir = agregado_metadata_dir(datadir, pesquisa_id, agregado_id)
    metadados_filename = f"metadados_{agregado_id:05}.json"
    localidades_filename = f"localidades_{agregado_id:05}.json"
    periodos_filename = f"periodos_{agregado_id:05}.json"
    return {
        "metadados": metadata_dir / metadados_filename,
        "localidades": metadata_dir / localidades_filename,
        "periodos": metadata_dir / periodos_filename,
    }
