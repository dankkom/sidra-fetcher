import argparse
import json

from pathlib import Path

from ibge_sidra_fetcher import utils


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-datadir", type=Path)
    args = parser.parse_args()
    return args


def read_metadata(datadir: Path, pesquisa_id: str, agregado_id: int):
    metadata_dir = datadir / pesquisa_id.lower() / f"agregado-{agregado_id:05}" / "metadata"
    fm = metadata_dir / f"metadados_{agregado_id}.json"
    fl = metadata_dir / f"localidades_{agregado_id}.json"
    fp = metadata_dir / f"periodos_{agregado_id}.json"

    metadados = json.load(fm.open(mode= "r", encoding="utf-8"))
    localidades = json.load(fl.open(mode= "r", encoding="utf-8"))
    periodos = json.load(fp.open(mode= "r", encoding="utf-8"))

    return metadados | {"localidades": localidades, "periodos": periodos}


def calculate(metadata):
    n = 0
    for _ in metadata["localidades"]:
        for _ in metadata["variaveis"]:
            for classificacao in metadata["classificacoes"]:
                n += len(classificacao["categorias"])
    return n


def main():
    args = get_args()
    sidra_agregados = utils.iter_sidra_agregados(args.datadir)
    cumsum = 0
    for agregado in sidra_agregados:
        pesquisa_id = agregado["pesquisa_id"]
        agregado_id = agregado["agregado_id"]
        metadata = read_metadata(
            datadir=args.datadir,
            pesquisa_id=pesquisa_id,
            agregado_id=agregado_id,
        )
        size = calculate(metadata)
        cumsum += size
        print(f"{pesquisa_id} {agregado_id: >5} {size: >15} {cumsum: >21}")


if __name__ == "__main__":
    main()
