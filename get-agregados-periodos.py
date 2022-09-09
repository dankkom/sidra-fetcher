
import json
import time

import httpx

from ibge_sidra_fetcher import api, config, utils


def main():

    c = httpx.Client()

    for i, agregado in enumerate(utils.iter_sidra_agregados(config.DATA_DIR)):
        pesquisa_id = agregado["pesquisa_id"]
        agregado_id = agregado["agregado_id"]
        output_file = agregado["metadata_files"]["periodos"]
        if output_file.exists():
            continue
        output_file.parent.mkdir(parents=True, exist_ok=True)
        print(f"{i: >6} {pesquisa_id} {agregado_id: >4}")
        try:
            periodos = api.agregados.get_agregado_periodos(agregado_id, c)
            periodos = json.loads(periodos.decode("utf-8"))
            print(f"Writing file {output_file}")
            output_file.write_text(json.dumps(periodos, indent=4))
        except Exception as e:
            print(e)
            time.sleep(5)


if __name__ == "__main__":
    main()
