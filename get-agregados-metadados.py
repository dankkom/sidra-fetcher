import json
import time

import httpx

from ibge_sidra_fetcher import api, config, utils


def main():

    c = httpx.Client()

    for agregado in utils.iter_sidra_agregados(config.DATA_DIR):
        agregado_id = agregado["agregado_id"]
        agregado_nome = agregado["agregado_nome"]
        output_file = agregado["metadata_files"]["metadados"]
        if output_file.exists():
            continue
        output_file.parent.mkdir(parents=True, exist_ok=True)
        print(f"    {agregado_id: >4} - {agregado_nome}")
        try:
            agregado_metadata = api.agregados.get_agregado_metadados(
                agregado_id,
                c,
            )
            agregado_metadata = json.loads(agregado_metadata.decode("utf-8"))
            print(f"Writing file {output_file}")
            output_file.write_text(json.dumps(agregado_metadata, indent=4))
        except Exception as e:
            print(e)
            time.sleep(5)


if __name__ == "__main__":
    main()
