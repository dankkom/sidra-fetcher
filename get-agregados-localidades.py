import json
import time
from pathlib import Path

import httpx

from ibge_sidra_fetcher import api, config, utils, path


def main():

    def get_niveis(metadados):
        administrativo = metadados["nivelTerritorial"]["Administrativo"]
        especial = metadados["nivelTerritorial"]["Especial"]
        ibge = metadados["nivelTerritorial"]["IBGE"]
        return set(administrativo + especial + ibge)

    c = httpx.Client()

    for agregado in utils.iter_sidra_agregados(config.DATA_DIR):
        pesquisa_id = agregado["pesquisa_id"]
        agregado_id = agregado["agregado_id"]
        agregado_nome = agregado["agregado_nome"]
        metadata_path = agregado["metadata_files"]
        metadados_filepath = metadata_path["metadados"]
        metadados = json.load(metadados_filepath.open("r", encoding="utf-8"))
        niveis = get_niveis(metadados)
        localidades_dir: Path = metadata_path["localidades"]
        localidades_dir.mkdir(parents=True, exist_ok=True)
        print(f"    {agregado_id: >4} - {agregado_nome}")
        for nivel in niveis:
            output_file = path.agregado_metadata_localidades_nivel_file(
                datadir=config.DATA_DIR,
                pesquisa_id=pesquisa_id,
                agregado_id=agregado_id,
                localidades_nivel=nivel,
            )
            if output_file.exists():
                continue
            try:
                agregado_localidades = api.agregados.get_agregado_localidades(
                    agregado_id,
                    nivel,
                    c,
                )
                print(f"Writing file {output_file}")
                output_file.write_text(json.dumps(agregado_localidades, indent=4))
            except Exception as e:
                print(e)
                time.sleep(5)


if __name__ == "__main__":
    main()
