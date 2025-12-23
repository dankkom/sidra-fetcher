
# sidra-fetcher

**sidra-fetcher** is a Python package for fetching and processing data and metadata from the IBGE's official APIs, including SIDRA and Agregados. It provides convenient, typed access to IBGE datasets for analysis, automation, and research.

## Features

- Download and parse IBGE Agregados and SIDRA data as Python dataclasses
- Typed access to metadata, periods, territorial levels, and variables
- URL builders and helpers for IBGE APIs
- Built-in retry logic for robust data fetching
- Utilities for statistics and data shape estimation

## Installation

```bash
pip install sidra-fetcher
# or, if using uv/poetry/hatch, add to your dependencies
```

## Usage Example

```python
from sidra_fetcher.fetcher import SidraClient

client = SidraClient()
# List available "agregados" (aggregates)
agregados = client.get_indice_agregados()
print(agregados[0])

# Fetch metadata for a specific aggregate
meta = client.get_agregado_metadata(1705)
print(meta.nome, meta.periodos)
```

## Project Structure

- `src/sidra_fetcher/fetcher.py`: Main HTTP client and high-level API
- `src/sidra_fetcher/api/agregados.py`: Dataclasses and helpers for Agregados API
- `src/sidra_fetcher/api/sidra.py`: URL builders and enums for SIDRA API
- `src/sidra_fetcher/stats.py`: Utilities for statistics and size estimation

## Supported APIs

- [IBGE API Docs](https://servicodados.ibge.gov.br/api/docs)
- [Agregados v3](https://servicodados.ibge.gov.br/api/docs/agregados?versao=3)
- [SIDRA](https://apisidra.ibge.gov.br) ([help](https://apisidra.ibge.gov.br/home/ajuda))

### Undocumented Acervos (Agregados)

- [Assuntos](https://servicodados.ibge.gov.br/api/v3/agregados?acervo=A)
- [Classificações](https://servicodados.ibge.gov.br/api/v3/agregados?acervo=C)
- [Nível Territorial](https://servicodados.ibge.gov.br/api/v3/agregados?acervo=N)
- [Períodos](https://servicodados.ibge.gov.br/api/v3/agregados?acervo=P)
- [Periodicidades](https://servicodados.ibge.gov.br/api/v3/agregados?acervo=E)
- [Variáveis](https://servicodados.ibge.gov.br/api/v3/agregados?acervo=V)

## Testing

Run the test suite with:

```bash
python -m unittest discover tests
```

## Contributing

Pull requests and issues are welcome! Please open an issue to discuss major changes. For local development, install dependencies and run tests as above.

## License

GNU GPLv3 License. See [LICENSE](LICENSE) for details.
