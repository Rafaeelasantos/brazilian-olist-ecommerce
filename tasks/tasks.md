# Prompt — Olist E-Commerce Lakeflow Pipeline (SDD)

---

## PERSONA

Sou um **Engenheiro de Dados Databricks**, especialista em **SDD (Spec-Driven Development)**.

---

## CONTEXTO

Como engenheiro de dados especialista, preciso criar um projeto de analytics com **Databricks Lakeflow Pipelines** completo e **production-ready**, baseado em **SDD (Spec-Driven Development)**.

O projeto segue a arquitetura **Medallion** (Bronze → Silver → Gold) e deve ser gerado integralmente a partir das especificações, sem inferências ou invenções externas.

---

## AÇÃO 1 — AGENTIC SENIOR DATA ENGINEER

> **START AGENTIC — SENIOR DATA ENGINEER**

Gere uma mensagem de início no formato:

```
START AGENTIC - SENIOR DATA ENGINEER
Lendo e analisando as especificações do projeto Olist E-Commerce...
```

---

### LEITURA E ANÁLISE OBRIGATÓRIA

Por favor, **LEIA** e **ANALISE** os seguintes itens antes de qualquer geração de código:

1. **Spec:** `specs/spec.md`
2. **Code Templates (Rules):** `templates\pipeline_templates.md`
3. **Workspace URL:** `https://dbc-f76716c3-b252.cloud.databricks.com/`

> ⚠️ **Todos os arquivos Python gerados devem seguir estritamente os templates definidos em `templates/pipeline_templates.md`.** Substitua cada `{{variable}}` pelo valor real da entidade sendo implementada. Não invente padrões fora dos templates.

---

### TAREFA

Com base **APENAS** nas especificações lidas, execute todos os passos contidos nelas e gere os seguintes artefatos:

#### Estrutura de arquivos esperada

```
olist_ecommerce/
├── databricks.yml
├── README.md
└── src/
    ├── bronze/
    │   ├── bronze_orders.py
    │   ├── bronze_order_items.py
    │   ├── bronze_customers.py
    │   ├── bronze_products.py
    │   └── bronze_product_category.py
    ├── silver/
    │   ├── silver_orders.py
    │   ├── silver_order_items.py
    │   ├── silver_customers.py
    │   ├── silver_products.py
    │   └── silver_product_category.py
    └── gold/
        ├── gold_dim_customers.py
        ├── gold_dim_products.py
        ├── gold_fct_orders.py
        └── gold_fct_order_items.py
```

#### Template mapping por arquivo

| File | Template | Notes |
|---|---|---|
| `bronze_*.py` | **Template 1** | Auto Loader via `spark.readStream.format("cloudFiles")` |
| `silver_orders.py` | **Template 2** | Standard streaming table |
| `silver_order_items.py` | **Template 2** | Standard streaming table |
| `silver_customers.py` | **Template 3** | SCD Type 2 — three separate calls |
| `silver_products.py` | **Template 2** | Standard streaming table + category join |
| `silver_product_category.py` | **Template 2** | Lookup table — no SCD2 |
| `gold_dim_customers.py` | **Template 4** | Filter `__END_AT IS NULL` |
| `gold_dim_products.py` | **Template 4** | Standard dimension |
| `gold_fct_orders.py` | **Template 5** | INNER JOIN with `dim_customers` |
| `gold_fct_order_items.py` | **Template 5** | INNER JOIN with `dim_products` |

#### Requisitos de implementação

- Todas as tabelas devem usar o formato **`catalog.schema.table`** — nunca o prefixo `LIVE.`
- **Bronze:** Auto Loader via `spark.readStream.format("cloudFiles")` — conforme Template 1
- **Silver streaming:** `dlt.read_stream()` — conforme Template 2
- **Silver SCD2 (customers):** três chamadas independentes (`@dlt.view` → `dlt.create_streaming_table()` → `dlt.apply_changes()`) — conforme Template 3
- **Gold dimensions:** `dlt.read()` (batch), `delta.enableChangeDataFeed = "false"` — conforme Template 4
- **Gold facts:** `dlt.read()` com `INNER JOIN` obrigatório na dimensão — conforme Template 5
- Incluir todos os **metadata fields** por camada (`_ingest_timestamp`, `_source_file`, `_processing_timestamp`, `_fact_processing_timestamp`, `_dimension_refresh_timestamp`)
- Aplicar todas as **Table Properties** especificadas (`quality`, `layer`, `domain`, `zOrderCols`, `enableChangeDataFeed`)
- Implementar **Data Quality Constraints** com `@dlt.expect_or_drop()` onde especificado
- Implementar **Derived Fields** e **Business Rules** conforme a spec
- `dim_customers` deve filtrar `.filter("__END_AT IS NULL")` para expor apenas registros correntes do SCD2

---

### RESTRIÇÕES

> ⚠️ **ATENÇÃO — OBRIGATÓRIO**

- ❌ **Não use placeholders** — substitua todos os `{{variables}}` pelos valores reais
- ❌ **Não invente campos** que não estão nas especificações
- ❌ **Não use o prefixo `LIVE.`** — sintaxe legada deprecada desde fevereiro de 2025
- ❌ **Não passe `schema="catalog.schema"`** para `@dlt.table()` ou `dlt.create_streaming_table()` — causa erro de sintaxe SQL
- ❌ **Não use `/tmp/`** para `cloudFiles.schemaLocation` — DBFS root está desabilitado neste workspace
- ✅ Use somente os campos, tipos e regras definidos na `spec.md`
- ✅ Referencie views de pré-processamento pelo **nome simples** (ex: `silver_customers_preprocessed`), sem qualificar com catalog ou schema
- ✅ `schemaLocation` deve sempre usar `/Volumes/workspace/default/ecommerce_raw_volume/_schemas/<entity>`

---

### ATENÇÃO — PÓS-IMPLEMENTAÇÃO

Após realizar todas as tarefas acima:

- [ ] Siga os templates em `templates\pipeline_templates.md` sem desvios
- [ ] Siga as instruções da spec corretamente, sem omissões
- [ ] **Revise toda a implementação** para garantir conformidade com a spec
- [ ] Execute a **AÇÃO 2 — AGENTIC DATA SOLUTIONS ARCHITECT: SUPERVISOR**

---

## AÇÃO 2 — AGENTIC DATA SOLUTIONS ARCHITECT: SUPERVISOR

> **START AGENTIC — DATA SOLUTIONS ARCHITECT: SUPERVISOR**

Gere uma mensagem de início no formato:

```
🔍 START AGENTIC - DATA SOLUTIONS ARCHITECT: SUPERVISOR
Iniciando revisão completa da implementação...
```

---

### ITENS DE REVISÃO OBRIGATÓRIA

Após a implementação do Senior Data Engineer, o Supervisor **DEVE** revisar:

#### 1. Revisão da implementação

- [ ] Revise **toda a implementação** para garantir que está correta e completa
- [ ] Revise as **primary keys** — todas as tabelas devem ter PKs definidas corretamente
- [ ] Revise as **foreign keys** — verifique se estão corretas e referenciando as tabelas certas
- [ ] Revise a **tabela fato `fct_orders`** — deve conter grain, FKs (`customer_id`) e os atributos de dimensão enriquecidos via JOIN
- [ ] Revise a **tabela fato `fct_order_items`** — deve conter grain, FKs (`order_id`, `product_id`) e os atributos de dimensão enriquecidos via JOIN
- [ ] Confirme que **ambas as facts** fazem `INNER JOIN` com suas respectivas dimensões — omitir o JOIN foi a causa raiz de atributos ausentes em execuções anteriores
- [ ] Revise os **tipos de dados** — confirme que estão em conformidade com a spec
- [ ] Revise se as **chaves primárias** estão corretamente definidas em todas as tabelas
- [ ] Revise a seção **ENTITY** contida nas especificações e confirme que todos os campos foram implementados
- [ ] Confirme que `silver_customers.py` contém **três chamadas independentes**: `@dlt.view` + `dlt.create_streaming_table()` + `dlt.apply_changes()`
- [ ] Confirme que `delta.enableChangeDataFeed = "false"` em todas as tabelas Gold
- [ ] Confirme que `continuous: false` está no bloco do resource do pipeline, **não** dentro de `configuration:`
- [ ] Confirme que o campo `target` no `databricks.yml` é `ecommerce_analytics`
- [ ] Confirme que nenhum arquivo usa `schema="catalog.schema"` nos decoradores DLT
- [ ] Confirme que todos os `cloudFiles.schemaLocation` apontam para `/Volumes/workspace/default/ecommerce_raw_volume/_schemas/<entity>` — nunca `/tmp/`

#### 2. Execução dos comandos Databricks Bundle

Execute os comandos na sequência abaixo e confirme sucesso em cada etapa:

```bash
# Validar a configuração antes de qualquer deploy
databricks bundle validate --profile rafaela.aws1992@gmail.com

# Deploy no target dev (default)
databricks bundle deploy --profile rafaela.aws1992@gmail.com

# Deploy explícito por target
databricks bundle deploy --profile rafaela.aws1992@gmail.com -t dev

# Gerar YAML a partir de um pipeline já existente no workspace
databricks bundle generate pipeline --existing-pipeline-id <pipeline-id>
```

- [ ] Confirme que a **validação** passou sem erros
- [ ] Confirme que o **deploy** foi realizado com sucesso
- [ ] Confirme que o **summary** reflete a estrutura esperada pela spec

---

## VALIDATION REQUIREMENTS

| # | Requisito | Detalhe |
|---|---|---|
| 1 | Full 3-part table names | Todas as tabelas usam `catalog.schema.table`. Nenhum prefixo `LIVE.` em lugar algum. |
| 2 | SCD2 — três chamadas independentes | `silver_customers.py` precisa de `@dlt.view` → `dlt.create_streaming_table()` → `dlt.apply_changes()` separados. |
| 3 | `except_column_list` e `track_history_except_column_list` independentes | Ambos os parâmetros devem aparecer no `apply_changes()` — não são intercambiáveis. |
| 4 | `fct_orders` faz JOIN com `dim_customers` | `gold_fct_orders.py` deve conter `INNER JOIN` com `dim_customers` em `customer_id`. |
| 5 | `fct_order_items` faz JOIN com `dim_products` | `gold_fct_order_items.py` deve conter `INNER JOIN` com `dim_products` em `product_id`. |
| 6 | Target schema é `ecommerce_analytics` | O campo `target` no DABs deve ser `ecommerce_analytics`. |
| 7 | `continuous` no bloco resource | `continuous: false` vai em `resources.pipelines.<name>`, nunca dentro de `configuration:`. |
| 8 | Nenhum job resource obrigatório | A spec valida um bundle com pipeline apenas; agendamento externo ou trigger manual é aceitável. |
| 9 | CDF desabilitado em Gold | `delta.enableChangeDataFeed` deve ser `"false"` em todas as tabelas Gold. |
| 10 | Auto Loader via `spark.readStream.format("cloudFiles")` | Bronze tables devem usar Template 1. Nunca usar `read_files()` (sintaxe SQL) em Python. |
| 11 | `databricks.yml` com `bundle`, `workspace`, `targets` e pipeline resource | O arquivo raiz deve declarar esses blocos; job resource separado é opcional. |
| 12 | `targets` com `mode: development` e `mode: production` | Usar `mode:` em vez de `development: true` (field legado). `mode: production` exige `run_as`. |
| 13 | Templates seguidos sem desvios | Cada arquivo deve corresponder ao template mapeado em `templates/pipeline_templates.md`. |

---

### ATENÇÃO — PÓS-IMPLEMENTAÇÃO

Após realizar todas as tarefas acima:

- [ ] Siga estritamente os templates de `templates/pipeline_templates.md`
- [ ] Siga as instruções da spec corretamente, sem omissões
- [ ] **Revise toda a implementação** para garantir conformidade com a spec
- [ ] Execute a **validação** do Databricks Bundle:
  ```bash
  databricks bundle validate
  ```
- [ ] Execute o **summary** do Databricks Bundle:
  ```bash
  databricks bundle summary
  ```

---

> **Version:** 1.1.0  
> **Updated:** 2026-05-25
