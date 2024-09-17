import httpx

from ibge_sidra_fetcher import config, fetcher, storage


def main():
    client = httpx.Client(timeout=config.TIMEOUT)

    sidra_agregados_filepath = storage.sidra_agregados_filepath(datadir=config.DATA_DIR)

    # Fetch and store the list of surveys if it doesn't exist
    if not sidra_agregados_filepath.exists():
        sidra_agregados_data = fetcher.sidra_agregados(client=client)
        storage.write_data(sidra_agregados_data, sidra_agregados_filepath)

    # Read the list of surveys
    sidra_agregados_data = storage.read_json(sidra_agregados_filepath)

    # Fetch and store the metadata of each agregado
    for pesquisa in sidra_agregados_data:
        pesquisa_id = pesquisa["id"]
        for agregado in pesquisa["agregados"]:
            agregado_id = agregado["id"]

            agregados_metadados_filepath = storage.agregado_metadados_filepath(
                datadir=config.DATA_DIR,
                pesquisa_id=pesquisa_id,
                agregado_id=agregado_id,
            )
            if agregados_metadados_filepath.exists():
                continue

            agregados_metadados_data = fetcher.sidra_agregados_metadados(
                agregado_id=agregado_id, client=client
            )

            storage.write_data(agregados_metadados_data, agregados_metadados_filepath)

            print(f"{pesquisa_id}/{agregado_id}")


if __name__ == "__main__":
    main()
