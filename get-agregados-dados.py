from queue import Queue

from ibge_sidra_fetcher import fetcher, stats, storage, utils
from ibge_sidra_fetcher.config import DATA_DIR
from ibge_sidra_fetcher.sidra import Agregado, Parametro


def main():
    q = Queue()
    for _ in range(4):
        ftchr = fetcher.Fetcher(q=q, data_dir=DATA_DIR)
        ftchr.start()
    for agregado in utils.iter_sidra_agregados(DATA_DIR):
        agregado: Agregado
        for periodo in agregado.periodos:
            m = stats.calculate_aggregate(agregado)
            tamanho_periodo = m["period_size"]
            if tamanho_periodo > 50_000:
                ...
            else:
                dest_filepath = storage.get_filepath(
                    datadir=DATA_DIR,
                    pesquisa_id=agregado.pesquisa.id,
                    agregado_id=agregado.id,
                    periodo_id=periodo.id,
                    modificacao=periodo.modificacao,
                )
                if dest_filepath.exists():
                    continue
                localidade_nivel = sorted(
                    set(loc.id_nivel.strip("N") for loc in agregado.localidades)
                )
                parameter = Parametro(
                    aggregate=str(agregado.id),
                    territories={
                        loc_nivel_id: ["all"] for loc_nivel_id in localidade_nivel
                    },
                    variables=["all"],
                    periods=[str(periodo.id)],
                    classifications={c.id: ["all"] for c in agregado.classificacoes},
                )
                task = {
                    "dest_filepath": dest_filepath,
                    "url": parameter.url(),
                }
                q.put(task)

    q.join()

    # client = httpx.Client()
    # n = 0
    # n = 0
    # for agregado in utils.iter_sidra_agregados(DATA_DIR):
    #     agregado: Agregado
    #     for periodo in agregado.periodos:
    #         m = stats.calculate_aggregate(agregado)
    #         tamanho_periodo = m["period_size"]
    #         if tamanho_periodo > 50_000:
    #             for localidade in agregado.localidades:
    #                 cod = " ".join(
    #                     [
    #                         f"{agregado.pesquisa.id}",
    #                         f"{agregado.id: >4}",
    #                         f"{periodo.id: <6}",
    #                         f"{localidade.id_nivel: >4}:{localidade.id: <9}",
    #                     ]
    #                 )
    #                 if m["localidade_size"] > 10_000:
    #                     log(f"[{cod}] -> {m['localidade_size']: >9} 😭")
    #                     continue
    #                 n += 1
    #                 print(
    #                     f"{n: >9} [{cod}] -> {m['localidade_size']: >9} 😑",
    #                 )
    #                 parameter = Parametro(
    #                     aggregate=str(agregado.id),
    #                     territories={
    #                         localidade.id_nivel.strip("N"): [str(localidade.id)],
    #                     },
    #                     variables=["all"],
    #                     periods=[str(periodo.id)],
    #                     classifications={
    #                         c.id: ["all"]
    #                         for c in agregado.classificacoes
    #                     },
    #                 )
    #                 url = parameter.url()
    #                 try:
    #                     data = fetcher.get_sidra_data(url, client)
    #                     storage.write_data(data, dest_filepath)
    #                 except Exception as e:
    #                     print("Error", e, parameter)

    #         else:
    #             n += 1
    #             print(
    #                 f"{n: >9} [{agregado.pesquisa.id}-{agregado.id:->4}-{periodo.id:->6}]",
    #                 "->",
    #                 f"{tamanho_periodo: >9} 😃",
    #             )
    #             dest_filepath = storage.get_filepath(
    #                 datadir=DATA_DIR,
    #                 pesquisa_id=agregado.pesquisa.id,
    #                 agregado_id=agregado.id,
    #                 periodo_id=periodo.id,
    #                 modificacao=periodo.modificacao,
    #             )
    #             if dest_filepath.exists():
    #                 continue
    #             localidade_nivel = sorted(
    #                 set(
    #                     loc.id_nivel.strip("N")
    #                     for loc in agregado.localidades
    #                 )
    #             )
    #             parameter = Parametro(
    #                 aggregate=str(agregado.id),
    #                 territories={
    #                     loc_nivel_id: ["all"]
    #                     for loc_nivel_id in localidade_nivel
    #                 },
    #                 variables=["all"],
    #                 periods=[str(periodo.id)],
    #                 classifications={c.id: ["all"] for c in agregado.classificacoes},
    #             )
    #             try:
    #                 data = fetcher.get_sidra_data(parameter.url(), client)
    #                 storage.write_data(data, dest_filepath)
    #             except Exception as e:
    #                 print("Error", e, parameter)


if __name__ == "__main__":
    main()
