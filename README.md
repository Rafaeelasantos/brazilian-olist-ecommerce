# Olist E-Commerce Analytics — Lakeflow Pipeline

Pipeline construído com Databricks Lakeflow, PySpark, Auto Loader e Declarative Automation Bundles (DABs), seguindo uma abordagem Spec-Driven Development (SDD).

---

## Fonte dos Dados

Este projeto utiliza o dataset público **Brazilian E-Commerce Public Dataset by Olist**, disponibilizado na plataforma [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

O conjunto de dados contém informações reais de um marketplace brasileiro e cobre o período aproximado de **2016 a 2018**, incluindo pedidos realizados em múltiplos marketplaces integrados pela Olist.

---

# Arquitetura

Este projeto implementa uma arquitetura Medalhão utilizando:

-  Bronze Layer → ingestão raw
-  Silver Layer → dados tratados e validados
-  Gold Layer → modelo dimensional pronto para analytics

O pipeline é executado utilizando Databricks Lakeflow Pipelines.

---

# Tecnologias Utilizadas

- Databricks Lakeflow Pipelines
- PySpark
- Auto Loader (`cloudFiles`)
- Unity Catalog
- Delta Lake
- Declarative Automation Bundles (DABs)
- Spec-Driven Development (SDD)

---

# Estrutura do Projeto

```text
olist_ecommerce/
├── databricks.yml
├── README.md
├── specs/
│   └── spec.md
├── tasks/
│   └── tasks.md
├── templates/
│   └── pipeline_templates.md
└── src/
    ├── bronze/
    ├── silver/
    └── gold/
````

---

# Spec-Driven Development (SDD)

Este projeto segue uma abordagem baseada em especificação técnica.

O fluxo de execução funciona da seguinte forma:

```text
tasks.md
   ↓
lê spec.md
   ↓
usa templates/pipeline_templates.md
   ↓
gera o código do pipeline
```

---

## Responsabilidade dos Arquivos

### `tasks/tasks.md`

Arquivo responsável pelo fluxo operacional de execução.

Define:

* sequência de execução
* validações obrigatórias
* passos de deploy
* restrições de implementação

---

### `specs/spec.md`

Define toda a especificação funcional e técnica do pipeline.

Contém:

* arquitetura
* entidades
* regras de qualidade
* requisitos
* regras de SCD Type 2
* configuração do Auto Loader
* configuração DABs
* convenções de nomenclatura

---

### `templates/pipeline_templates.md`

Define templates reutilizáveis e padrões de implementação.

Contém:

* templates Bronze
* templates Silver
* templates Gold
* padrões SCD Type 2
* joins obrigatórios
* metadados
* boas práticas

Este arquivo funciona como referência de como o código deve ser gerado e usa variáveis para ser reaproveitado em qualquer outro projeto.

---

# Camadas Medalhão

## Bronze

Camada responsável pela ingestão raw utilizando Auto Loader.

Características:

* ingestão streaming
* rastreamento de schema
* metadados de ingestão
* camada imutável

Principal tecnologia:

```python
spark.readStream.format("cloudFiles")
```

---

## Silver

Camada curada e validada.

Características:

* validações de qualidade
* campos derivados
* padronização
* SCD Type 2 para clientes

Principal tecnologia:

```python
dlt.apply_changes()
```

---

## Gold

Camada analítica pronta para consumo de negócio.

Características:

* dimensões
* tabelas fato
* star schema
* datasets para BI e analytics

---

# Auto Loader

Todas as tabelas Bronze utilizam Auto Loader com controle explícito de schema.

Exemplo:

```python
spark.readStream.format("cloudFiles")
```

# SCD Type 2

A dimensão de clientes é implementada utilizando:

```python
dlt.apply_changes()
```

A implementação exige:

1. view de pré-processamento
2. `dlt.create_streaming_table()`
3. `dlt.apply_changes()`

---

# Declarative Automation Bundles (DABs)

O deploy é realizado utilizando Databricks DABs.

Arquivo principal:

```text
databricks.yml
```

Responsável por:

* definição do bundle
* targets
* pipelines
* workspace
* configuração de deploy

---

# Deploy

## Validar bundle

```bash
databricks bundle validate --profile <profile>
```

---

## Deploy

```bash
databricks bundle deploy --profile <profile>
```

---

## Deploy para target específico

```bash
databricks bundle deploy --profile <profile> -t dev
```

---

# Arquitetura de Dados

## Bronze

| Tabela             | Descrição                |
| ------------------ | ------------------------ |
| bronze_orders      | ingestão raw de pedidos  |
| bronze_order_items | ingestão raw de itens    |
| bronze_customers   | ingestão raw de clientes |
| bronze_products    | ingestão raw de produtos |

---

## Silver

| Tabela             | Descrição               |
| ------------------ | ----------------------- |
| silver_orders      | pedidos tratados        |
| silver_order_items | itens tratados          |
| silver_customers   | clientes com SCD Type 2 |
| silver_products    | produtos enriquecidos   |

---

## Gold

| Tabela          | Descrição            |
| --------------- | -------------------- |
| dim_customers   | dimensão de clientes |
| dim_products    | dimensão de produtos |
| fct_order_items | fato de itens        |

---

# Decisões Técnicas

## Uso de nomes completos

Todas as tabelas utilizam:

```text
catalog.schema.table
```

Exemplo:

```python
workspace.silver.silver_orders
```

---

O projeto utiliza o novo modo de publicação do Lakeflow.

A sintaxe legada `LIVE.` não é utilizada.

---

Projeto desenvolvido utilizando Databricks Lakeflow Pipelines e Spec-Driven Development (SDD).

```
