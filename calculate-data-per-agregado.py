from ibge_sidra_fetcher import stats, utils
from ibge_sidra_fetcher.config import DATA_DIR


def main():
    for i, agregado in enumerate(utils.iter_sidra_agregados(DATA_DIR), 1):
        if not agregado:
            continue

        n = stats.calculate_aggregate(agregado)
        msg = [
            f"{i: >6}",
            f"{n['pesquisa_id']}-{n['agregado_id']: <5}",
            f"Localidades: {n['n_niveis_territoriais']: >2}:{n['n_localidades']: <5}",
            f"Variaveis: {n['n_variaveis']: >4}",
            f"Dimensões: {n['n_classificacoes']: >2}:{n['n_dimensoes']: <7}",
            f"Períodos: {n['n_periodos']: >3}",
            f"Tamanho Período: {n['period_size']: >15,}".replace(",", "."),
            f"Tamanho Total: {n['total_size']: >15,}".replace(",", "."),
        ]
        print(" ".join(msg))


if __name__ == "__main__":
    main()
