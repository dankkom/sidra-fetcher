import json
import sys
import unittest
from unittest.mock import MagicMock

# Mock external dependencies that might be missing
sys.modules["httpx"] = MagicMock()
sys.modules["tenacity"] = MagicMock()
sys.modules["tenacity.retry"] = MagicMock()
sys.modules["tenacity.stop"] = MagicMock()
sys.modules["tenacity.wait"] = MagicMock()


# Mock the decorators from tenacity to just return the function
def mock_retry(*args, **kwargs):
    def decorator(f):
        return f

    return decorator


sys.modules["tenacity"].retry = mock_retry

from sidra_fetcher.api.agregados import AcervoEnum
from sidra_fetcher.fetcher import SidraClient


class TestFetcher(unittest.TestCase):
    def test_get_indice_pesquisas_agregados(self):
        mock_response = [
            {
                "id": "P1",
                "nome": "Pesquisa 1",
                "agregados": [{"id": 1, "nome": "Agregado 1"}],
            }
        ]

        mock_httpx = sys.modules["httpx"]
        mock_client_instance = mock_httpx.Client.return_value
        mock_client_instance.stream.return_value.__enter__.return_value.iter_bytes.return_value = [
            json.dumps(mock_response).encode("utf-8")
        ]

        client = SidraClient()
        result = client.get_indice_pesquisas_agregados()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, "P1")
        self.assertEqual(result[0].agregados[0].id, 1)

    def test_get_agregado_metadados(self):
        mock_response = {
            "id": 123,
            "nome": "Agregado Teste",
            "URL": "http://url",
            "pesquisa": "Pesquisa Teste",
            "assunto": "Assunto Teste",
            "periodicidade": {
                "frequencia": "mensal",
                "inicio": "202001",
                "fim": "202012",
            },
            "nivelTerritorial": {
                "Administrativo": ["N1"],
                "Especial": [],
                "IBGE": [],
            },
            "variaveis": [
                {"id": 1, "nome": "V1", "unidade": "u", "sumarizacao": []}
            ],
            "classificacoes": [
                {
                    "id": 1,
                    "nome": "C1",
                    "sumarizacao": {"status": True, "excecao": []},
                    "categorias": [
                        {"id": 10, "nome": "Cat1", "unidade": None, "nivel": 1}
                    ],
                }
            ],
        }

        mock_httpx = sys.modules["httpx"]
        mock_client_instance = mock_httpx.Client.return_value
        mock_client_instance.stream.return_value.__enter__.return_value.iter_bytes.return_value = [
            json.dumps(mock_response).encode("utf-8")
        ]

        client = SidraClient()
        agregado = client.get_agregado_metadados(123)

        self.assertEqual(agregado.id, 123)
        self.assertEqual(agregado.nome, "Agregado Teste")
        self.assertEqual(len(agregado.variaveis), 1)
        self.assertEqual(len(agregado.classificacoes), 1)

    def test_get_agregado_periodos(self):
        mock_response = [
            {
                "id": "202001",
                "literals": ["Jan 2020"],
                "modificacao": "01/01/2020",
            }
        ]

        mock_httpx = sys.modules["httpx"]
        mock_client_instance = mock_httpx.Client.return_value
        mock_client_instance.stream.return_value.__enter__.return_value.iter_bytes.return_value = [
            json.dumps(mock_response).encode("utf-8")
        ]

        client = SidraClient()
        periodos = client.get_agregado_periodos(123)

        self.assertEqual(len(periodos), 1)
        self.assertEqual(periodos[0].id, "202001")

    def test_get_agregado_localidades(self):
        mock_response = [
            {
                "id": "1",
                "nome": "Loc1",
                "nivel": {"id": "N1", "nome": "Nivel 1"},
            }
        ]

        mock_httpx = sys.modules["httpx"]
        mock_client_instance = mock_httpx.Client.return_value
        mock_client_instance.stream.return_value.__enter__.return_value.iter_bytes.return_value = [
            json.dumps(mock_response).encode("utf-8")
        ]

        client = SidraClient()
        localidades = client.get_agregado_localidades(123, "N1")

        self.assertEqual(len(localidades), 1)
        self.assertEqual(localidades[0].id, "1")
        self.assertEqual(localidades[0].nivel.id, "N1")

    def test_get_acervo(self):
        mock_response = {"some": "data"}

        mock_httpx = sys.modules["httpx"]
        mock_client_instance = mock_httpx.Client.return_value
        mock_client_instance.stream.return_value.__enter__.return_value.iter_bytes.return_value = [
            json.dumps(mock_response).encode("utf-8")
        ]

        client = SidraClient()
        data = client.get_acervo(AcervoEnum.ASSUNTO)

        self.assertEqual(data, mock_response)


if __name__ == "__main__":
    unittest.main()
