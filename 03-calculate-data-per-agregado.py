import argparse
import csv
from pathlib import Path

from ibge_sidra_fetcher import stats, utils


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Directory to store the fetched data",
    )
    return parser.parse_args()


def main():
    args = get_args()
    data_dir = args.data_dir

    with open("ibge-sidra-agregados.csv", "w", encoding="utf-8", newline="\n") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "pesquisa_id",
                "agregado_id",
                "n_niveis_territoriais",
                "n_localidades",
                "n_variaveis",
                "n_classificacoes",
                "n_dimensoes",
                "n_periodos",
                "period_size",
                "localidade_size",
                "variavel_size",
                "total_size",
            ],
        )
        writer.writeheader()

        for i, agregado in enumerate(utils.iter_sidra_agregados(data_dir), 1):
            if not agregado:
                continue

            n = stats.calculate_aggregate(agregado)
            msg = [
                f"{i: >6}",
                f"{n['pesquisa_id']}-{n['agregado_id']:05}",
                f"Localidades: {n['n_niveis_territoriais']: >2}:{n['n_localidades']: <5}",
                f"Variáveis: {n['n_variaveis']: >4}",
                f"Dimensões: {n['n_classificacoes']: >2}:{n['n_dimensoes']: <7}",
                f"Períodos: {n['n_periodos']: >3}",
                f"Tamanho Período: {n['period_size']: >15,}".replace(",", "_"),
                f"Tamanho Total: {n['total_size']: >15,}".replace(",", "_"),
            ]
            print(" ".join(msg))
            del n["stat_localidades"]
            writer.writerow(n)


if __name__ == "__main__":
    main()
