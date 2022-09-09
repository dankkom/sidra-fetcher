from ibge_sidra_fetcher import api, config, utils


def main():

    for agregado in utils.iter_sidra_agregados(config.DATA_DIR):
        metadata = utils.read_metadata(agregado)
        pesquisa_id = agregado["pesquisa_id"]
        agregado_id = agregado["agregado_id"]
        for periodo in metadata["periodos"]:
            m = utils.calculate_n_dimensions(metadata)
            tamanho_periodo = (
                m["n_localidades"]
                * m["n_variaveis"]
                * m["n_categorias"]
            )
            if tamanho_periodo > 10_000:
                for localidade in metadata["localidades"]:
                    tamanho_localidade = m["n_variaveis"] * m["n_categorias"]
                    print(
                        pesquisa_id,
                        agregado_id,
                        periodo["id"],
                        localidade["id"],
                        tamanho_localidade,
                    )


if __name__ == "__main__":
    main()
