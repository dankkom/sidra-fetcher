# Coletor de dados do SIDRA/IBGE

Functions to request resources from IBGE's APIs

IBGE's APIs list:

- https://servicodados.ibge.gov.br/api/docs
- https://servicodados.ibge.gov.br/api/docs/agregados?versao=3
- https://apisidra.ibge.gov.br
- https://apisidra.ibge.gov.br/home/ajuda

## Workflow

1. list-sidra-agregados (pesquisa_id, pesquisa_nome, agregado_id, agregado_nome)
2. get-agregados-metadata
3. get-periodos
4. fetch-data
5. archive

## Dimensions

- nivelTerritorial
- variaveis
- classificacoes
  - categorias
