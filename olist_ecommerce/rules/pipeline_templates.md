---
description: Code templates for Olist E-Commerce Lakeflow Pipeline (PySpark / DLT). Use these parameterised models whenever generating or reviewing pipeline files. Replace every {{variable}} with the actual value for the entity being implemented.
alwaysApply: true
---

# Lakeflow Pipeline — Code Templates (PySpark)

> All templates follow the patterns mandated by `specs/spec.md`.  
> Replace every `{{variable}}` placeholder before writing a file.  
> Never use the `LIVE.` prefix. Always use 3-part names: `catalog.schema.table`.

---

## Variables Reference

| Variable | Example values |
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
| `{{metadata_timestamp_field}}` | `_dimension_refresh_timestamp` (dim) \| `_fact_processing_timestamp` (fact) |

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
    comment="Bronze layer — raw ingestion of {{entity}} via Auto Loader",
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

## Template 2 — Silver Streaming Table (standard entity)

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
    comment="Silver layer — cleaned {{entity}}",
)
@dlt.expect_or_drop("{{constraint_name}}", "{{constraint_expression}}")
def {{table_name}}():
    return (
        dlt.read_stream("{{bronze_source}}")
        .select(
            # --- source fields ---
            "{{pk_field}}",
            # ... remaining fields ...
            "_ingest_timestamp",
            # --- derived fields ---
            # col("field_a") + col("field_b")).alias("derived_field"),
            current_timestamp().alias("_processing_timestamp"),
        )
    )
```

---

## Template 3 — Silver SCD Type 2 (Customers)

> Three independent calls in the same file — do NOT nest them.

```python
import dlt
from pyspark.sql.functions import current_timestamp, concat, col, lit


# Step 1: Preprocessing view — apply constraints and derived fields before CDC
@dlt.view(name="{{table_name}}_preprocessed")
@dlt.expect_or_drop("{{constraint_name_1}}", "{{constraint_expression_1}}")
@dlt.expect_or_drop("{{constraint_name_2}}", "{{constraint_expression_2}}")
def {{table_name}}_preprocessed():
    return (
        dlt.read_stream("{{bronze_source}}")
        # derived fields
        .withColumn(
            "{{derived_field}}",
            # expression using col() / lit() / concat() etc.
            concat(col("{{field_a}}"), lit(", "), col("{{field_b}}")),
        )
    )


# Step 2: Declare the target streaming table (REQUIRED before apply_changes)
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


# Step 3: Apply SCD Type 2 changes
dlt.apply_changes(
    target="workspace.silver.{{table_name}}",
    source="{{table_name}}_preprocessed",   # simple name — no catalog prefix
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
    comment="Gold layer — {{entity}} dimension",
)
def {{table_name}}():
    return (
        dlt.read("{{silver_source}}")
        # .filter("__END_AT IS NULL")  # uncomment for SCD2 sources
        .select(
            "{{pk_field}}",
            # ... remaining dimension fields ...
        )
        .withColumn("_dimension_refresh_timestamp", current_timestamp())
    )
```

---

## Template 5 — Gold Fact Table (with dimension join)

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
    comment="Gold layer — {{entity}} fact table",
)
def {{table_name}}():
    fact = dlt.read("{{silver_source}}")
    dim  = dlt.read("{{gold_source}}")   # e.g. workspace.gold.dim_customers

    return (
        fact.join(dim, on="{{join_key}}", how="inner")
        .select(
            fact["{{pk_field}}"],
            # ... remaining fact fields ...
            # --- dimension attributes ---
            dim["{{dim_field_1}}"],
            dim["{{dim_field_2}}"],
        )
        .withColumn("_fact_processing_timestamp", current_timestamp())
    )
```

---

## Rules

1. **3-part table names** — always `catalog.schema.table`. Never `LIVE.*`.
2. **Bronze** — `spark.readStream.format("cloudFiles")`. Declare `cloudFiles.schemaLocation` explicitly. Point to the **directory**, not the file.
3. **Silver streaming** — `dlt.read_stream("{{bronze_source}}")`.
4. **Silver SCD2** — three separate calls: `@dlt.view` → `dlt.create_streaming_table()` → `dlt.apply_changes()`.
5. **Gold** — `dlt.read()` (batch). CDF **disabled** (`"false"`).
6. **Fact tables** — must `INNER JOIN` their dimension. Never omit the join.
7. **`schema=` parameter** — never pass `schema="catalog.schema"` to `@dlt.table()` or `dlt.create_streaming_table()`. That parameter is for column DDL only.
8. **`except_column_list`** and **`track_history_except_column_list`** are independent — both must appear in `apply_changes()`.
9. **`schemaLocation`** — always use `/Volumes/workspace/default/ecommerce_raw_volume/_schemas/{{entity}}`. Never `/tmp/`.
10. **`delta.enableChangeDataFeed`** — `"true"` for Bronze and Silver; `"false"` for Gold.
