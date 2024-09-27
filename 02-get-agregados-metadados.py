import queue

from ibge_sidra_fetcher import dispatcher, fetcher, storage



def main():
    q = queue.Queue()
    worker = fetcher.Fetcher(q)
    worker.start()

    sidra_agregados_filepath = storage.sidra_agregados_filepath(datadir=config.DATA_DIR)
    sidra_agregados_data = storage.read_json(sidra_agregados_filepath)

    fetcher_worker = fetcher.Fetcher(q)
    fetcher_worker.start()

    for pesquisa in sidra_agregados_data:
        pesquisa_id = pesquisa["id"]
        for agregado in pesquisa["agregados"]:
            agregado_id = agregado["id"]
            task = dispatcher.metadados(data_dir, pesquisa_id, agregado_id)
            q.put(task)
            task = dispatcher.periodos(data_dir, pesquisa_id, agregado_id)
            q.put(task)
    q.join()


if __name__ == "__main__":
    main()
