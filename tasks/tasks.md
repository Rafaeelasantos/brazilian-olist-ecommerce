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
🚀 START AGENTIC - SENIOR DATA ENGINEER
Lendo e analisando as especificações do projeto Olist E-Commerce...
```

---

### LEITURA E ANÁLISE OBRIGATÓRIA

Por favor, **LEIA** e **ANALISE** os seguintes itens antes de qualquer geração de código:

1. **Spec:** `brazilian-ecommerce/specs/spec.md`
2. **Workspace URL:** `https://dbc-f76716c3-b252.cloud.databricks.com/`

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
    │   ├── bronze_orders.sql
    │   ├── bronze_order_items.sql
    │   ├── bronze_customers.sql
    │   ├── bronze_products.sql
    │   └── bronze_product_category.sql
    ├── silver/
    │   ├── silver_orders.sql
    │   ├── silver_order_items.sql
    │   ├── silver_customers.sql
    │   ├── silver_products.sql
    │   └── silver_product_category.sql
    └── gold/
        ├── gold_dim_customers.sql
        ├── gold_dim_products.sql
        ├── gold_fct_orders.sql
        └── gold_fct_order_items.sql
```

#### Requisitos de implementação

- Todas as tabelas devem usar o formato **`catalog.schema.table`**
- Implementar **Auto Loader** via `read_files()` com formato CSV em todas as fontes Bronze — **não usar `cloud_files` nem `cloudFiles.*`** (sintaxe exclusiva de Python/Scala; em SQL usa-se `FROM STREAM read_files(...)`)
- Aplicar **SCD Type 2** via `AUTO CDC with FLOW` **somente para `customers`** na camada Silver — `orders`, `order_items`, `products` e `product_category` são streaming tables simples
- Incluir todos os **metadata fields** por camada (`_ingest_timestamp`, `_source_file`, `_processing_timestamp`, `_fact_processing_timestamp`, `_dimension_refresh_timestamp`)
- Aplicar todas as **Table Properties** especificadas (`quality`, `layer`, `domain`, `zOrderCols`, `enableChangeDataFeed`)
- Implementar **Data Quality Constraints** com `DROP ROW` onde especificado
- Implementar **Derived Fields** e **Business Rules** conforme a spec
- `product_category` deve ser tratada como tabela de lookup estática — sem CDC, sem SCD2
- `dim_customers` deve filtrar `__END_AT IS NULL` para expor apenas registros correntes do SCD2

---

### RESTRIÇÕES

> ⚠️ **ATENÇÃO — OBRIGATÓRIO**

- ❌ **Não use placeholders**
- ❌ **Não invente campos que não estão nas especificações**
- ❌ **Não use o prefixo `LIVE.`** — sintaxe legada deprecada desde fevereiro de 2025
- ❌ **Não use `cloudFiles.schemaLocation` ou `cloudFiles.schemaEvolutionMode` em SQL** — gerenciados automaticamente pelo runtime
- ✅ Use somente os campos, tipos e regras definidos na `spec.md`
- ✅ Referencie views de pré-processamento temporárias pelo **nome simples** (ex: `silver_customers_preprocessed`), sem qualificar com catalog ou schema

---

### ATENÇÃO — PÓS-IMPLEMENTAÇÃO

Após realizar todas as tarefas acima:

- [ ] Siga as instruções da spec corretamente, sem omissões
- [ ] **Revise toda a implementação** para garantir conformidade com a spec
- [ ] Execute a **AÇÃO 2 — AGENTIC DATA SOLUTIONS ARCHITECT: SUPERVISOR** :

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
- [ ] Revise se as **chaves estrangeiras** estão corretamente definidas e íntegras
- [ ] Revise se as **chaves primárias** estão corretamente definidas em todas as tabelas
- [ ] Revise a seção **ENTITY** contida nas especificações e confirme que todos os campos foram implementados
- [ ] Confirme que `silver_customers.sql` contém **dois statements separados**: `CREATE OR REFRESH STREAMING TABLE` + `CREATE FLOW … AS AUTO CDC INTO`
- [ ] Confirme que `delta.enableChangeDataFeed = 'false'` em todas as tabelas Gold (materialized views não suportam CDF)
- [ ] Confirme que `continuous: false` está no bloco do resource do pipeline, **não** dentro de `configuration:`
- [ ] Confirme que o campo `target` no `databricks.yml` é `ecommerce_analytics`, **não** `observability`

#### 2. Execução dos comandos Databricks Bundle

Execute os comandos na sequência abaixo e confirme sucesso em cada etapa:

```bash
# Validar a configuração antes de qualquer deploy
databricks bundle validate --profile rafaela.aws1992@gmail.com

# Deploy no target dev (default)
databricks bundle deploy --profile rafaela.aws1992@gmail.com

# Deploy explícito por target
databricks bundle deploy --profile rafaela.aws1992@gmail.com -t dev

# Rodar o job manualmente após deploy
databricks bundle run ride_analytics_job -t dev

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
| 2 | AUTO CDC — dois statements | `silver_customers.sql` precisa de `CREATE OR REFRESH STREAMING TABLE` seguido de `CREATE FLOW … AS AUTO CDC INTO` separados. |
| 3 | `COLUMNS` e `TRACK HISTORY ON` são independentes | Ambas as cláusulas devem aparecer no `CREATE FLOW` e não são intercambiáveis. |
| 4 | `fct_orders` faz JOIN com `dim_customers` | `gold_fct_orders.sql` deve conter `INNER JOIN dim_customers ON customer_id`. |
| 5 | `fct_order_items` faz JOIN com `dim_products` | `gold_fct_order_items.sql` deve conter `INNER JOIN dim_products ON product_id`. |
| 6 | Target schema é `ecommerce_analytics` | O campo `target` no DABs deve ser `ecommerce_analytics`, não `observability`. |
| 7 | `continuous` no bloco resource | `continuous: false` vai em `resources.pipelines.<name>`, nunca dentro de `configuration:`. |
| 8 | Nenhum job resource obrigatório | A spec valida um bundle com pipeline apenas; agendamento externo ou trigger manual é aceitável. |
| 9 | CDF desabilitado em Gold | `delta.enableChangeDataFeed` deve ser `'false'` (ou omitido) para materialized views Gold. |
| 10 | Auto Loader via `read_files()` em SQL | Bronze tables devem usar `FROM STREAM read_files(path, format => 'csv', ...)`. Nunca usar `cloud_files` ou `cloudFiles.*` em SQL. |
| 11 | `databricks.yml` com `bundle`, `workspace`, `targets` e pipeline resource | O arquivo raiz deve declarar esses blocos; job resource separado é opcional. |
| 12 | `targets` com `mode: development` e `mode: production` | Usar `mode:` em vez de `development: true` (field legado). `mode: production` exige `run_as`. |

---


---

### ATENÇÃO — PÓS-IMPLEMENTAÇÃO

Após realizar todas as tarefas acima:

- [ ] Siga estritamente os padrões definidos em `RULES`
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

> **Version:** 1.0.0
> **Created:** 2026-05-22
