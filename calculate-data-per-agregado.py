from ibge_sidra_fetcher import fetcher


def main():
    for i, n in enumerate(fetcher.calculate_data_per_agregado(), 1):
        msg = [
            f"{i: >6}",
            f"{n['pesquisa_id']}-{n['agregado_id']: <5}",
            f"Localidades: {n['n_niveis_territoriais']: >2}:{n['n_localidades']: <5}",
            f"Variaveis: {n['n_variaveis']: >4}",
            f"Categorias: {n['n_categorias']: >4}",
            f"Períodos: {n['n_periodos']: >3}",
            f"Tamanho Período: {n['period_size']: >11,}".replace(",", "."),
            f"Tamanho Total: {n['total_size']: >11,}".replace(",", "."),
        ]
        print(" ".join(msg))


if __name__ == "__main__":
    main()
