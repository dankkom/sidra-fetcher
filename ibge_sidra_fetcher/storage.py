import datetime as dt
import json
from pathlib import Path

from .sidra.agregado import (
    Agregado,
    Categoria,
    Classificacao,
    Localidade,
    Periodo,
    Pesquisa,
    Variavel,
)


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
    localidade_id: int = None,
    variavel_id: int = None,
) -> Path:
    d = agregado_dir(datadir, pesquisa_id, agregado_id)
    partition = [f"{periodo_id}"]
    if localidade_id:
        partition.append(f"{localidade_id}")
    if variavel_id:
        partition.append(f"{variavel_id}")
    partition = "-".join(partition)
    filename = f"{agregado_id}_{partition}_{modificacao:%Y%m%d}.json"
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


def read_periodos(filepath: Path) -> list[Periodo]:
    periodos = []
    parse_modificacao = lambda x: dt.datetime.strptime(x, "%d/%m/%Y").date()
    for per in read_json(filepath):
        p = Periodo(
            id=int(per["id"]),
            literals=per["literals"],
            modificacao=parse_modificacao(per["modificacao"]),
        )
        periodos.append(p)
    return periodos


def read_localidades(filepath: Path) -> list[Localidade]:
    localidades = []
    for loc in read_json(filepath):
        l = Localidade(
            id=int(loc["id"]),
            nome=loc["nome"],
            id_nivel=loc["nivel"]["id"],
            nome_nivel=loc["nivel"]["nome"],
        )
        localidades.append(l)
    return localidades


def read_metadados(datadir: Path, pesquisa_id: str, agregado_id: int) -> Agregado:
    files = agregado_metadata_files(
        datadir=datadir,
        pesquisa_id=pesquisa_id,
        agregado_id=agregado_id,
    )
    localidades = []
    for f in files["localidades"]:
        localidades.extend(read_localidades(f))
    periodos = read_periodos(files["periodos"])
    data = read_json(files["metadados"])
    variaveis = [
        Variavel(id=int(v["id"]), nome=v["nome"], unidade=v["unidade"])
        for v in data["variaveis"]
    ]
    classificacoes = [
        Classificacao(
            id=cla["id"],
            nome=cla["nome"],
            categorias=[
                Categoria(
                    id=int(cat["id"]),
                    nome=cat["nome"],
                    unidade=cat["unidade"],
                    nivel=int(cat["nivel"]),
                )
                for cat in cla["categorias"]
            ],
        )
        for cla in data["classificacoes"]
    ]
    a = Agregado(
        id=int(data["id"]),
        nome=data["nome"],
        url=data["URL"],
        pesquisa=Pesquisa(id=pesquisa_id, nome=data["pesquisa"]),
        assunto=data["assunto"],
        periodicidade_frequencia=data["periodicidade"]["frequencia"],
        variaveis=variaveis,
        classificacoes=classificacoes,
        periodos=periodos,
        localidades=localidades,
    )
    return a
