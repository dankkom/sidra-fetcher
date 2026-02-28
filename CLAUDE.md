# CLAUDE.md — sidra-fetcher

This file provides AI assistants with a comprehensive guide to the `sidra-fetcher`
codebase: its purpose, structure, conventions, and development workflows.

---

## Project Overview

**sidra-fetcher** is a Python package (v0.2.0, GPLv3) that provides typed,
convenient access to Brazilian statistical data from IBGE's official APIs:

- **IBGE Agregados v3** — `https://servicodados.ibge.gov.br/api/v3/agregados`
- **SIDRA** — `https://apisidra.ibge.gov.br`

Target users are data scientists and researchers working with Brazilian IBGE
datasets. The package handles HTTP retry logic, streaming downloads, data
parsing into typed dataclasses, URL construction/parsing, and data shape
estimation.

---

## Repository Structure

```
sidra-fetcher/
├── pyproject.toml              # Project metadata, dependencies, Ruff config
├── README.md                   # Public-facing documentation
├── LICENSE                     # GNU GPLv3
├── .python-version             # Pins Python 3.13
├── src/
│   └── sidra_fetcher/
│       ├── __init__.py         # Logger setup (NullHandler)
│       ├── agregados.py        # Dataclasses + URL builders for Agregados API
│       ├── fetcher.py          # SidraClient HTTP client
│       ├── sidra.py            # SIDRA URL builder/parser, Parametro model
│       ├── stats.py            # Size/shape estimation utilities
│       └── reader.py           # JSON parsers + metadata flattening
└── tests/
    ├── test_agregados.py       # URL builder tests
    ├── test_fetcher.py         # SidraClient tests (mocked httpx + tenacity)
    ├── test_sidra.py           # SIDRA URL parsing tests
    └── test_stats.py           # Statistics utility tests
```

---

## Module Descriptions

### `agregados.py`
Defines **dataclasses** that mirror IBGE Agregados API response structures and
**URL builder** helpers.

Key dataclasses (all use `@dataclass`):
- `Periodo` — period id, literals, modification date (`dt.date`)
- `NivelTerritorial` — territorial level id and name
- `Localidade` — locality with its territorial level
- `Variavel` — variable id, name, unit, summarization
- `Categoria` — category/member of a classification
- `ClassificacaoSumarizacao` — summarization rules (status + exceptions)
- `Classificacao` — classification with categories
- `Pesquisa` — survey reference (id + name)
- `Periodicidade` — frequency, start, end
- `AgregadoNivelTerritorial` — three lists of territorial levels (administrativo, especial, ibge)
- `Agregado` — top-level aggregate combining all above
- `IndiceAgregado` — lightweight id/name pair
- `IndicePesquisaAgregados` — survey grouping a list of `IndiceAgregado`
- `AcervoEnum(StrEnum)` — collection types: `A`ssunto, `C`lassificacao, `N`ivelTerritorial, `P`eriodo, P`E`riodicidade, `V`ariavel

URL builders:
- `build_url_agregados()` → base URL
- `build_url_metadados(agregado_id)` → `/{id}/metadados`
- `build_url_periodos(agregado_id)` → `/{id}/periodos`
- `build_url_localidades(agregado_id, localidades_nivel)` → `/{id}/localidades/{nivel}`
- `build_url_acervos(acervo)` → base URL with `?acervo=<value>`

### `fetcher.py`
Provides `SidraClient`, the main HTTP client. Uses `httpx` for HTTP and
`tenacity` for retry logic.

```python
client = SidraClient(timeout=60)           # or use as context manager
with SidraClient() as client:
    surveys = client.get_indice_pesquisas_agregados()
    meta    = client.get_agregado_metadados(1705)
    periods = client.get_agregado_periodos(1705)
    locs    = client.get_agregado_localidades(1705, "N6")
    full    = client.get_agregado(1705)    # composes all of the above
    acervo  = client.get_acervo(AcervoEnum.VARIAVEL)
```

- `get()` streams the response with `httpx.Client.stream("GET", url)`
- Methods with `@retry` use `stop_after_attempt(3)`; `get_agregado_periodos` adds `wait_exponential(min=3, max=30)`
- Raises `ConnectionError` if the response is empty or the request fails

### `sidra.py`
URL construction and parsing for the SIDRA `/values` endpoint.

**Enums:**
- `Formato` — output format (`A`=codes+names, `C`=codes, `N`=names, `U`=codes+names+units)
- `Precisao` — decimal precision (`S`=standard, `M`=max, `D0`–`D20`=fixed decimals)

**`Parametro` class** models all SIDRA request segments:
- `agregado` → `/t/{id}`
- `territorios: dict[str, list[str]]` → `/n{level}/{ids}`; empty list → `/all`
- `variaveis: list[str]` → `/v/{ids}`; empty → `/v/all`
- `periodos: list[str]` → `/p/{ids}`; empty → `/p/all`
- `classificacoes: dict[str, list[str]]` → `/c{id}/{values}`
- `cabecalho: bool` → `/h/y` or `/h/n`
- `formato: Formato` → `/f/{value}`
- `decimais: dict[str, Precisao]` → `/d/{value}` or `/d/v{id}%20{prec},...`
- `url()` renders the full request URL
- `assign(name, value)` returns an immutable copy with one field replaced

**Parsing functions** (each returns `(raw_match, parsed_value)`):
- `parse_aggregate(url)`, `parse_territories(url)`, `parse_variables(url)`
- `parse_periods(url)`, `parse_classifications(url)`, `parse_header(url)`
- `parse_format(url)`, `parse_decimal(url)`
- `parse_url(url)` — returns a dict with all segments
- `parameter_from_url(url)` — reconstructs a `Parametro` from a URL string

### `stats.py`
Utilities to estimate data size before performing bulk downloads.

- `get_stat_localidades(agregado)` → `dict[str, int]` mapping level id to count
- `get_n_dimensoes(agregado)` → product of category counts across all classifications
- `calculate_aggregate(agregado)` → dict with `n_localidades`, `n_variaveis`,
  `n_classificacoes`, `n_dimensoes`, `n_periodos`, `period_size`, `total_size`,
  `localidade_size`, `variavel_size`, `stat_localidades`, `pesquisa_id`, `agregado_id`

### `reader.py`
Parses raw API JSON into typed dataclasses and flattens hierarchical metadata.

- `read_metadados(data)` → `Agregado` (periodos and localidades set to `[]`)
- `read_periodos(data)` → `list[Periodo]` (parses date with `%d/%m/%Y`)
- `read_localidades(data)` → `list[Localidade]`
- `flatten_aggregate_metadata(aggregate_metadata)` — generator yielding flat dicts
  representing every variable × classification-category combination; keys include
  `agregado`, `pesquisa`, `assunto`, `frequencia`, `url_agregado`, `D4C`/`D4N`
  (variable), `D5C`/`D5N`/`C5C`/`C5N` (first classification), `MN` (unit), `nivel`
- `flatten_surveys_metadata(surveys_metadata)` → flat list of `{pesquisa_id,
  pesquisa, agregado_id, agregado}` dicts

Internal helpers (not part of the public API):
- `_iter_variaveis_metadata()` — loops variables and delegates to classifications
- `_iter_classificacoes_metadata()` — recursively generates category combinations;
  skips categories with `unit is None` at the last classification level

---

## Development Setup

**Python version:** 3.13 (pinned via `.python-version`)

**Install dependencies** (using `uv` or `pip`):

```bash
pip install -e ".[dev]"
# or
uv pip install -e .
```

**Runtime dependencies:**
- `httpx >= 0.28.1`
- `tenacity >= 9.1.2`

**Build system:** `hatchling`

---

## Running Tests

```bash
python -m unittest discover tests
```

Tests use `unittest` from the standard library. External dependencies (`httpx`,
`tenacity`) are mocked at the top of `test_fetcher.py` using `sys.modules` and
`unittest.mock.MagicMock`.

Test coverage targets:
- `test_agregados.py` — URL builder functions and `AcervoEnum` values
- `test_fetcher.py` — all `SidraClient` public methods via mocked HTTP
- `test_sidra.py` — `Parametro` construction, URL rendering, and all parse functions
- `test_stats.py` — `get_stat_localidades`, `get_n_dimensoes`, `calculate_aggregate`

---

## Code Conventions

### Style
- **Line length:** 79 characters (enforced by Ruff)
- **Import sorting:** enabled via `ruff lint.extend-select = ["I"]` (isort rules)
- All source files include the **GPL v3 copyright header**
- Module-level docstrings explain the module's purpose

### Structure
- New public API types belong in `agregados.py` as `@dataclass` classes
- HTTP logic belongs in `fetcher.py`
- URL utilities for SIDRA belong in `sidra.py`
- Parsing and flattening logic belongs in `reader.py`
- Size estimation utilities belong in `stats.py`

### Dataclasses
- All data containers are plain `@dataclass` (not frozen); avoid adding
  business logic to them
- Date fields use `datetime.date` (not `datetime.datetime`)
- Optional units use `str | None`

### HTTP / Fetching
- Always use `httpx.Client.stream()` (not `.get()`) to handle large responses
- Retry decorators go on `SidraClient` methods, not on `get()`
- Raise `ConnectionError` for empty or failed responses

### Logging
- `__init__.py` configures a `NullHandler` on the package logger — consumers
  opt in to logging by adding their own handlers
- Use `logger.info()` at the start of each fetch, `logger.debug()` for timing

### Tests
- Mock `httpx` and `tenacity` at the `sys.modules` level before importing
  `sidra_fetcher`
- Use `unittest.TestCase` subclasses; one test file per source module
- Construct minimal fixture data (dicts/lists) inline in each test method

---

## Key API Endpoints

| Endpoint | URL pattern |
|---|---|
| Agregados index | `GET https://servicodados.ibge.gov.br/api/v3/agregados` |
| Aggregate metadata | `GET .../agregados/{id}/metadados` |
| Aggregate periods | `GET .../agregados/{id}/periodos` |
| Aggregate localities | `GET .../agregados/{id}/localidades/{nivel}` |
| Acervos (collections) | `GET .../agregados?acervo={A\|C\|N\|P\|E\|V}` |
| SIDRA data | `GET https://apisidra.ibge.gov.br/values/t/{id}/n{level}/{ids}/...` |

---

## Common Patterns

### Fetching a complete aggregate

```python
from sidra_fetcher.fetcher import SidraClient

with SidraClient() as client:
    agregado = client.get_agregado(1705)
    print(agregado.nome, len(agregado.periodos), len(agregado.localidades))
```

### Building a SIDRA request URL

```python
from sidra_fetcher.sidra import Parametro, Formato, Precisao

p = Parametro(
    agregado="6723",
    territorios={"1": ["all"]},
    variaveis=["1394", "1395"],
    periodos=["all"],
    classificacoes={"844": ["all"]},
    formato=Formato.A,
    decimais={"1394": Precisao.D2, "1395": Precisao.D2},
)
print(p.url())
```

### Flattening aggregate metadata for analysis

```python
from sidra_fetcher.reader import flatten_aggregate_metadata

raw = client.get(build_url_metadados(1705))
for record in flatten_aggregate_metadata(raw):
    print(record["agregado"], record["D4N"], record["MN"])
```

### Estimating data size before download

```python
from sidra_fetcher.stats import calculate_aggregate

stats = calculate_aggregate(agregado)
print(stats["total_size"], stats["n_periodos"], stats["n_localidades"])
```

---

## Things to Be Aware Of

- The `Agregado.periodos` and `Agregado.localidades` fields are always `[]`
  when returned by `read_metadados()` or `get_agregado_metadados()`. They are
  populated only by `get_agregado()`, which makes the additional API calls.
- `Pesquisa.id` is set to `""` (empty string) when constructing an `Agregado`
  from the metadata endpoint, because the metadata response does not include
  the survey id.
- The `_iter_classificacoes_metadata` dimension numbering starts at `n=4`
  because dimensions 1–3 are implicitly reserved for aggregate, survey, and
  subject level data.
- `AcervoEnum` docstring appears after the enum members — this is intentional
  (historical artifact); do not move the docstring above the members.
- Date parsing in `read_periodos` / `get_agregado_periodos` uses Brazilian
  format `%d/%m/%Y`. Ensure any new date parsing follows the same convention.
- `Parametro.assign()` uses `setattr` and returns a shallow copy; nested
  mutable objects (dicts, lists) are shared between the original and the copy.
