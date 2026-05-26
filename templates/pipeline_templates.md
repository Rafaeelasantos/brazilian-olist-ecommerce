---
description: Templates de código para o pipeline Lakeflow do Olist E-Commerce (PySpark / DLT). Use estes modelos parametrizados sempre que gerar ou revisar arquivos de pipeline. Substitua cada {{variável}} pelo valor real da entidade sendo implementada.
alwaysApply: true
---

# Lakeflow Pipeline — Templates de Código (PySpark)

> Todos os templates seguem os padrões definidos em `specs/spec.md`.  
> Substitua cada placeholder `{{variável}}` antes de escrever um arquivo.  
> Nunca use o prefixo `LIVE.`. Use sempre nomes com 3 partes: `catalog.schema.table`.

---

## Referência de Variáveis

| Variável | Exemplos de valores |
|---|---|
| `{{entity}}` | `orders`, `order_items`, `customers`, `products`, `product_category` |
| `{{domain}}` | `orders`, `order_items`, `customers`, `products`, `product_category` |
| `{{table_name}}` | `bronze_orders`, `silver_orders`, `dim_customers`, `fct_orders` |
| `{{layer}}` | `bronze`, `silver`, `gold` |
| `{{pk_field}}` | `order_id`, `customer_id`, `product_id` |
| `{{source_path}}` | `/Volumes/workspace/default/ecommerce_raw_volume/olist_orders_dataset.csv/` |
| `{{schema_location}}` | `/Volumes/workspace/default/ecommerce_raw_volume/_schemas/{{entity}}` |
| `{{bronze_source}}` | `workspace.bronze.bronze_orders` |
| `{{silver_source}}` | `workspace.silver.silver_orders` |
| `{{gold_source}}` | `workspace.gold.dim_customers` |
| `{{cdf_enabled}}` | `"true"` (Bronze/Silver) \| `"false"` (Gold) |
| `{{metadata_timestamp_field}}` | `_dimension_refresh_timestamp` (dimensão) \| `_fact_processing_timestamp` (fato) |

---

## Template 1 — Bronze Streaming Table (Auto Loader)

```python
import dlt
from pyspark.sql.functions import current_timestamp, col


@dlt.table(
    name="{{table_name}}",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "domain": "{{domain}}",
        "pipelines.autoOptimize.zOrderCols": "{{pk_field}}",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Camada Bronze — ingestão bruta de {{entity}} via Auto Loader",
)
def {{table_name}}():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("inferSchema", "false")
        .option(
            "cloudFiles.schemaLocation",
            "{{schema_location}}",
        )
        .option("cloudFiles.schemaEvolutionMode", "none")
        .load("{{source_path}}")
        .select(
            "*",
            current_timestamp().alias("_ingest_timestamp"),
            col("_metadata.file_path").alias("_source_file"),
        )
    )
```

---

## Template 2 — Silver Streaming Table (entidade padrão)

```python
import dlt
from pyspark.sql.functions import current_timestamp, col


@dlt.table(
    name="{{table_name}}",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "{{domain}}",
        "pipelines.autoOptimize.zOrderCols": "{{pk_field}}",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Camada Silver — {{entity}} limpo e padronizado",
)
@dlt.expect_or_drop("{{constraint_name}}", "{{constraint_expression}}")
def {{table_name}}():
    return (
        dlt.read_stream("{{bronze_source}}")
        .select(
            # --- campos da fonte ---
            "{{pk_field}}",
            # ... demais campos ...
            "_ingest_timestamp",
            # --- campos derivados ---
            # col("field_a") + col("field_b")).alias("derived_field"),
            current_timestamp().alias("_processing_timestamp"),
        )
    )
```

---

## Template 3 — Silver SCD Type 2 (Customers)

> Três chamadas independentes no mesmo arquivo — NÃO as aninhe.

```python
import dlt
from pyspark.sql.functions import current_timestamp, concat, col, lit


# Passo 1: View de pré-processamento — aplica constraints e campos derivados antes do CDC
@dlt.view(name="{{table_name}}_preprocessed")
@dlt.expect_or_drop("{{constraint_name_1}}", "{{constraint_expression_1}}")
@dlt.expect_or_drop("{{constraint_name_2}}", "{{constraint_expression_2}}")
def {{table_name}}_preprocessed():
    return (
        dlt.read_stream("{{bronze_source}}")
        # campos derivados
        .withColumn(
            "{{derived_field}}",
            # expressão usando col() / lit() / concat() etc.
            concat(col("{{field_a}}"), lit(", "), col("{{field_b}}")),
        )
    )


# Passo 2: Declara a streaming table de destino (OBRIGATÓRIO antes de apply_changes)
dlt.create_streaming_table(
    name="{{table_name}}",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "{{domain}}",
        "pipelines.autoOptimize.zOrderCols": "{{pk_field}}",
        "delta.enableChangeDataFeed": "true",
    },
)


# Passo 3: Aplica as mudanças SCD Type 2
dlt.apply_changes(
    target="workspace.silver.{{table_name}}",
    source="{{table_name}}_preprocessed",   # nome simples — sem prefixo de catalog
    keys=["{{pk_field}}"],
    sequence_by=col("_ingest_timestamp"),
    stored_as_scd_type=2,
    except_column_list=["_processing_timestamp", "_ingest_timestamp"],
    track_history_except_column_list=["_processing_timestamp", "_ingest_timestamp"],
)
```

---

## Template 4 — Gold Dimension Table

```python
import dlt
from pyspark.sql.functions import current_timestamp


@dlt.table(
    name="{{table_name}}",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "{{domain}}",
        "pipelines.autoOptimize.zOrderCols": "{{pk_field}}",
        "delta.enableChangeDataFeed": "false",
    },
    comment="Camada Gold — dimensão de {{entity}}",
)
def {{table_name}}():
    return (
        dlt.read("{{silver_source}}")
        # .filter("__END_AT IS NULL")  # descomentar para fontes SCD2
        .select(
            "{{pk_field}}",
            # ... demais campos da dimensão ...
        )
        .withColumn("_dimension_refresh_timestamp", current_timestamp())
    )
```

---

## Template 5 — Gold Fact Table (com join de dimensão)

```python
import dlt
from pyspark.sql.functions import current_timestamp


@dlt.table(
    name="{{table_name}}",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "{{domain}}",
        "pipelines.autoOptimize.zOrderCols": "{{pk_field}}",
        "delta.enableChangeDataFeed": "false",
    },
    comment="Camada Gold — tabela fato de {{entity}}",
)
def {{table_name}}():
    fact = dlt.read("{{silver_source}}")
    dim  = dlt.read("{{gold_source}}")   # ex: workspace.gold.dim_customers

    return (
        fact.join(dim, on="{{join_key}}", how="inner")
        .select(
            fact["{{pk_field}}"],
            # ... demais campos do fato ...
            # --- atributos da dimensão ---
            dim["{{dim_field_1}}"],
            dim["{{dim_field_2}}"],
        )
        .withColumn("_fact_processing_timestamp", current_timestamp())
    )
```

---

## Regras

1. **Nomes com 3 partes** — sempre `catalog.schema.table`. Nunca `LIVE.*`.
2. **Bronze** — `spark.readStream.format("cloudFiles")`. Declare `cloudFiles.schemaLocation` explicitamente. Aponte para o **diretório**, não para o arquivo.
3. **Silver streaming** — `dlt.read_stream("{{bronze_source}}")`.
4. **Silver SCD2** — três chamadas separadas: `@dlt.view` → `dlt.create_streaming_table()` → `dlt.apply_changes()`.
5. **Gold** — `dlt.read()` (batch). CDF **desabilitado** (`"false"`).
6. **Tabelas fato** — devem fazer `INNER JOIN` com sua dimensão. Nunca omita o join.
7. **Parâmetro `schema=`** — nunca passe `schema="catalog.schema"` para `@dlt.table()` ou `dlt.create_streaming_table()`. Esse parâmetro é exclusivo para DDL de colunas.
8. **`except_column_list`** e **`track_history_except_column_list`** são independentes — ambos devem aparecer em `apply_changes()`.
9. **`schemaLocation`** — use sempre `/Volumes/workspace/default/ecommerce_raw_volume/_schemas/{{entity}}`. Nunca `/tmp/`.
10. **`delta.enableChangeDataFeed`** — `"true"` para Bronze e Silver; `"false"` para Gold.
