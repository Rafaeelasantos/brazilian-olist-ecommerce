# Olist E-Commerce — Lakeflow Pipeline Specification (PySpark)

**Version:** 2.0 — May 2025

| Field | Value |
|---|---|
| **Project Name** | Olist E-Commerce Analytics |
| **Catalog** | `workspace` |
| **Schema (Bronze / Silver / Gold)** | `bronze` / `silver` / `gold` |
| **Volume Base Path** | `/Volumes/workspace/default/ecommerce_raw_volume` |
| **Pipeline Name** | `olist_ecommerce_dev_lakeflow` |
| **Environment** | dev |

---

## 1. Data Architecture — Medallion

All tables use Unity Catalog no formato `catalog.schema.table`.

| Layer | Name | Purpose |
|---|---|---|
| 🥉 Bronze | Raw Layer | Immutable ingestion of CSV files via Auto Loader (`cloudFiles`). Metadata fields added here. |
| 🥈 Silver | Curated Layer | Cleaned and standardised data. SCD Type 2 applied to Customers via `dlt.apply_changes()`. Derived fields computed here. |
| 🥇 Gold | Business Layer | Star schema with fact and dimension tables. Aggregated metrics ready for reporting. |

---

## 2. Data Sources — Bronze Ingestion

> ⚠️ **Critical — Python Auto Loader syntax:** Em pipelines Python/PySpark, o Auto Loader é invocado via `spark.readStream.format("cloudFiles")` com as opções `cloudFiles.format`, `cloudFiles.schemaLocation`, etc. O `schemaLocation` **deve ser declarado explicitamente** no código Python — diferente do SQL, onde é gerenciado automaticamente pelo runtime.

### Source Paths

> ⚠️ **Critical — Volume directory structure:** No Unity Catalog Volume deste workspace, cada dataset CSV é armazenado **dentro de um diretório**, não como um arquivo avulso. O nome do diretório inclui a extensão `.csv` (e.g., `olist_products_dataset.csv/` é um diretório, não um arquivo). O Auto Loader deve apontar para o **diretório** (com barra final `/`), não para um caminho de arquivo. Passar um caminho de arquivo inexistente causa `FileNotFoundException`.
>
> Estrutura real no volume:
> ```
> /Volumes/workspace/default/ecommerce_raw_volume/
> ├── olist_orders_dataset.csv/
> │   └── olist_orders_dataset.csv          ← arquivo dentro do diretório
> ├── olist_order_items_dataset.csv/
> │   └── olist_order_items_dataset.csv
> ├── olist_customers_dataset.csv/
> │   └── olist_customers_dataset.csv
> ├── olist_products_dataset.csv/
> │   └── olist_products_dataset.csv
> └── product_category/
>     └── product_category_name_translation.csv
> ```

| Entity | Directory (Auto Loader target) | Source Path |
|---|---|---|
| `orders` | `olist_orders_dataset.csv/` | `/Volumes/workspace/default/ecommerce_raw_volume/olist_orders_dataset.csv/` |
| `order_items` | `olist_order_items_dataset.csv/` | `/Volumes/workspace/default/ecommerce_raw_volume/olist_order_items_dataset.csv/` |
| `customers` | `olist_customers_dataset.csv/` | `/Volumes/workspace/default/ecommerce_raw_volume/olist_customers_dataset.csv/` |
| `products` | `olist_products_dataset.csv/` | `/Volumes/workspace/default/ecommerce_raw_volume/olist_products_dataset.csv/` |
| `product_category` | `product_category/` | `/Volumes/workspace/default/ecommerce_raw_volume/product_category/` |

### `cloudFiles` Options — CSV (Python)

| Option | Value | Note |
|---|---|---|
| `cloudFiles.format` | `"csv"` | Required |
| `header` | `"true"` | First row as column names |
| `delimiter` | `","` | Field separator |
| `inferSchema` | `"false"` | Use declared schema for production stability |
| `cloudFiles.schemaLocation` | `"/Volumes/workspace/default/ecommerce_raw_volume/_schemas/<entity>"` | Obrigatório em Python; gerenciado por entidade. **Nunca use `/tmp/` — DBFS root está desabilitado neste workspace. Sempre usar Unity Catalog Volume.** |
| `cloudFiles.schemaEvolutionMode` | `"none"` | Recomendado para produção |

### Python Template — Bronze Streaming Table

> ⚠️ **Critical — `schema` parameter in `@dlt.table()` / `dlt.create_streaming_table()`:** O parâmetro `schema` nessas funções é reservado para **definição de colunas DDL** (ex.: `"order_id STRING, price DOUBLE"`). Ele **não** define onde a tabela é publicada. Passar `schema="workspace.bronze"` (ou qualquer valor `catalog.schema`) faz o runtime tentar interpretar a string como DDL de colunas, causando um erro de sintaxe SQL:
> ```
> CREATE TABLE `workspace`.`ecommerce_analytics`.`bronze_orders` (workspace.bronze)
> Syntax error at or near '.'
> ```
> A localização de publicação (catalog + schema) é controlada pelos campos `catalog` e `target` no `databricks.yml`. **Nunca passe `schema=` com valores `catalog.schema` nos decoradores DLT.**

```python
import dlt
from pyspark.sql.functions import current_timestamp, col

@dlt.table(
    name="bronze_orders",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "domain": "orders",
        "pipelines.autoOptimize.zOrderCols": "order_id",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Bronze layer — raw ingestion of orders via Auto Loader",
)
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("inferSchema", "false")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/ecommerce_raw_volume/_schemas/orders")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .load("/Volumes/workspace/default/ecommerce_raw_volume/olist_orders_dataset.csv/")
        .select(
            "*",
            current_timestamp().alias("_ingest_timestamp"),
            col("_metadata.file_path").alias("_source_file"),
        )
    )
```

> ℹ️ `_metadata.file_path` é uma coluna de metadados exposta pelo Auto Loader. Ela deve ser referenciada via `col("_metadata.file_path")` antes de qualquer transformação que remova colunas de metadados.

---

## 3. Entities

### 3.1 Orders

| Attribute | Value |
|---|---|
| **Type** | Fact source |
| **Domain** | `orders` |
| **PII Level** | Low |

#### Fields

| Field | Type | Description | PII / Tags |
|---|---|---|---|
| `order_id` | StringType | Unique order identifier (PK) | — |
| `customer_id` | StringType | Key to the customers dataset | — |
| `order_status` | StringType | Order status (`delivered`, `shipped`, etc.) | — |
| `order_purchase_timestamp` | TimestampType | Purchase timestamp | — |
| `order_approved_at` | TimestampType | Payment approval timestamp | — |
| `order_delivered_carrier_date` | TimestampType | Posting date — when handed to the logistics partner | — |
| `order_delivered_customer_date` | TimestampType | Actual delivery date to customer | — |
| `order_estimated_delivery_date` | TimestampType | Estimated delivery date shown to customer at purchase | — |

#### Derived Fields — Silver

| Field | Logic |
|---|---|
| `delivery_delay_days` | `datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date"))` — positive = late |
| `order_processing_days` | `datediff(col("order_approved_at"), col("order_purchase_timestamp"))` |
| `is_late_delivery` | `when(col("delivery_delay_days") > 0, True).otherwise(False)` |

#### Data Quality Constraints

```python
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_status", "order_status IS NOT NULL")
```

#### Python Template — Silver Orders

```python
import dlt
from pyspark.sql.functions import current_timestamp, datediff, col, when

@dlt.table(
    name="silver_orders",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "orders",
        "pipelines.autoOptimize.zOrderCols": "order_id",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_status", "order_status IS NOT NULL")
def silver_orders():
    delay = datediff(
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date"),
    ).alias("delivery_delay_days")

    return (
        dlt.read_stream("workspace.bronze.bronze_orders")
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "_ingest_timestamp",
            delay,
            datediff(col("order_approved_at"), col("order_purchase_timestamp")).alias(
                "order_processing_days"
            ),
            current_timestamp().alias("_processing_timestamp"),
        )
        .withColumn(
            "is_late_delivery",
            when(col("delivery_delay_days") > 0, True).otherwise(False),
        )
    )
```

---

### 3.2 Order Items

| Attribute | Value |
|---|---|
| **Type** | Fact source |
| **Domain** | `order_items` |
| **PII Level** | Low |

#### Fields

| Field | Type | Description | PII / Tags |
|---|---|---|---|
| `order_id` | StringType | Order unique identifier (FK → orders) | — |
| `order_item_id` | IntegerType | Sequential item number within the order | — |
| `product_id` | StringType | Product unique identifier (FK → products) | — |
| `seller_id` | StringType | Seller unique identifier | — |
| `shipping_limit_date` | TimestampType | Seller shipping limit date | — |
| `price` | DoubleType | Item price | — |
| `freight_value` | DoubleType | Item freight value | — |

#### Derived Fields — Silver

| Field | Logic |
|---|---|
| `total_item_value` | `col("price") + col("freight_value")` |

#### Data Quality Constraints

```python
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_price", "price >= 0")
```

#### Python Template — Silver Order Items

```python
import dlt
from pyspark.sql.functions import current_timestamp, col

@dlt.table(
    name="silver_order_items",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "order_items",
        "pipelines.autoOptimize.zOrderCols": "order_id",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_price", "price >= 0")
def silver_order_items():
    return (
        dlt.read_stream("workspace.bronze.bronze_order_items")
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
            "_ingest_timestamp",
            (col("price") + col("freight_value")).alias("total_item_value"),
            current_timestamp().alias("_processing_timestamp"),
        )
    )
```

---

### 3.3 Customers (Dimension — SCD Type 2)

| Attribute | Value |
|---|---|
| **Type** | Dimension — SCD Type 2 |
| **Domain** | `customers` |
| **PII Level** | 🔒 High |

#### Fields

| Field | Type | Description | PII / Tags |
|---|---|---|---|
| `customer_id` | StringType | Key to the orders dataset — unique per order | — |
| `customer_unique_id` | StringType | Unique identifier of the customer (person) | — |
| `customer_zip_code_prefix` | StringType | First five digits of customer zip code | 🔒 PII |
| `customer_city` | StringType | Customer city name | — |
| `customer_state` | StringType | Customer state | — |

#### Derived Fields — Silver

| Field | Logic |
|---|---|
| `customer_location` | `concat(col("customer_city"), lit(", "), col("customer_state"))` |

#### Data Quality Constraints

Aplicadas na view de pré-processamento antes do `apply_changes`:

```python
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_unique_id", "customer_unique_id IS NOT NULL")
```

---

### 3.4 Products

| Attribute | Value |
|---|---|
| **Type** | Dimension |
| **Domain** | `products` |
| **PII Level** | Low |

#### Fields

| Field | Type | Description | PII / Tags |
|---|---|---|---|
| `product_id` | StringType | Unique product identifier (PK) | — |
| `product_category_name` | StringType | Root category name in Portuguese (FK → product_category) | — |
| `product_name_lenght` | IntegerType | Number of characters in the product name | — |
| `product_description_lenght` | IntegerType | Number of characters in the product description | — |
| `product_photos_qty` | IntegerType | Number of published product photos | — |
| `product_weight_g` | DoubleType | Product weight in grams | — |
| `product_length_cm` | DoubleType | Product length in centimetres | — |
| `product_height_cm` | DoubleType | Product height in centimetres | — |
| `product_width_cm` | DoubleType | Product width in centimetres | — |

#### Derived Fields — Silver

| Field | Logic |
|---|---|
| `product_volume_cm3` | `col("product_length_cm") * col("product_height_cm") * col("product_width_cm")` |
| `product_category_name_english` | Join com `workspace.silver.silver_product_category` em `product_category_name` |

#### Data Quality Constraints

```python
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_weight", "product_weight_g > 0")
```

---

### 3.5 Product Category (Reference / Lookup)

| Attribute | Value |
|---|---|
| **Type** | Reference / Lookup |
| **Domain** | `product_category` |
| **PII Level** | Low |

#### Fields

| Field | Type | Description | PII / Tags |
|---|---|---|---|
| `product_category_name` | StringType | Category name in Portuguese (PK) | — |
| `product_category_name_english` | StringType | Category name in English | — |

> ℹ️ Dataset estático de lookup — não requer SCD Type 2. Carregue como streaming table no Silver e faça join com products para enriquecer com o nome em inglês.

---

## 4. Silver Layer — SCD Type 2 (Customers)

> ⚠️ **Critical:** Em PySpark, o SCD Type 2 é implementado com `dlt.apply_changes()`. Isso requer **duas definições separadas**:
> 1. `@dlt.table` — declara a tabela alvo.
> 2. `dlt.apply_changes()` — aplica as mudanças CDC.
>
> Ambos devem estar no mesmo arquivo Python, mas são chamadas independentes — não decoradores aninhados.

> ℹ️ `apply_changes()` não aceita filtros diretamente. Se precisar de filtragem ou validação de qualidade, crie uma view de pré-processamento com `@dlt.view` antes de chamar `apply_changes`.

### Python Template — Silver SCD Type 2 (Customers)

```python
import dlt
from pyspark.sql.functions import current_timestamp, concat, col, lit

# Step 1: Preprocessing view com filtros e derived fields
@dlt.view(name="silver_customers_preprocessed")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_unique_id", "customer_unique_id IS NOT NULL")
def silver_customers_preprocessed():
    return (
        dlt.read_stream("workspace.bronze.bronze_customers")
        .withColumn(
            "customer_location",
            concat(col("customer_city"), lit(", "), col("customer_state")),
        )
    )

# Step 2: Declaração da tabela alvo (OBRIGATÓRIA antes do apply_changes)
dlt.create_streaming_table(
    name="silver_customers",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "customers",
        "pipelines.autoOptimize.zOrderCols": "customer_id",
        "delta.enableChangeDataFeed": "true",
    },
)

# Step 3: Aplicação do CDC com SCD Type 2
dlt.apply_changes(
    target="workspace.silver.silver_customers",
    source="silver_customers_preprocessed",
    keys=["customer_id"],
    sequence_by=col("_ingest_timestamp"),
    stored_as_scd_type=2,
    except_column_list=["_processing_timestamp", "_ingest_timestamp"],
    track_history_except_column_list=["_processing_timestamp", "_ingest_timestamp"],
)
```

> ℹ️ `except_column_list` controla quais campos da fonte são escritos no target. `track_history_except_column_list` controla quais campos, quando alterados, disparam uma nova linha SCD2. São parâmetros independentes e ambos devem ser declarados. Os campos SCD2 gerados automaticamente são `__START_AT`, `__END_AT` e `__IS_CURRENT`.

### SCD Type 2 — `apply_changes()` Configuration

| Parameter | Value |
|---|---|
| `target` | `"workspace.silver.silver_customers"` |
| `source` | `"silver_customers_preprocessed"` |
| `keys` | `["customer_id"]` |
| `sequence_by` | `col("_ingest_timestamp")` |
| `stored_as_scd_type` | `2` |
| `except_column_list` | `["_processing_timestamp", "_ingest_timestamp"]` |
| `track_history_except_column_list` | `["_processing_timestamp", "_ingest_timestamp"]` |
| **SCD Fields generated** | `__START_AT`, `__END_AT`, `__IS_CURRENT` |

---

## 5. Gold Layer — Star Schema

### 5.1 Dimensions

| Dimension | Source | Filter | Join Key to Fact |
|---|---|---|---|
| `dim_customers` | `workspace.silver.silver_customers` | `__END_AT IS NULL` (SCD2 current rows only) | `dim_customers.customer_id = fct_orders.customer_id` |
| `dim_products` | `workspace.silver.silver_products` | No filter needed | `dim_products.product_id = fct_order_items.product_id` |

> ℹ️ No PySpark, `dlt.read()` é usado para leituras batch (Materialized Views e Dimensions). `dlt.read_stream()` é reservado para streaming tables.

### Python Template — `dim_customers`

```python
import dlt
from pyspark.sql.functions import current_timestamp

@dlt.table(
    name="dim_customers",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "customers",
        "pipelines.autoOptimize.zOrderCols": "customer_id",
        "delta.enableChangeDataFeed": "false",
    },
)
def dim_customers():
    return (
        dlt.read("workspace.silver.silver_customers")
        .filter("__END_AT IS NULL")  # SCD2 — somente linha corrente
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "customer_location",
        )
        .withColumn("_dimension_refresh_timestamp", current_timestamp())
    )
```

### Python Template — `dim_products`

```python
import dlt
from pyspark.sql.functions import current_timestamp, col

@dlt.table(
    name="dim_products",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "products",
        "pipelines.autoOptimize.zOrderCols": "product_id",
        "delta.enableChangeDataFeed": "false",
    },
)
def dim_products():
    products = dlt.read("workspace.silver.silver_products")
    return (
        products.select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_volume_cm3",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "product_photos_qty",
        )
        .withColumn("_dimension_refresh_timestamp", current_timestamp())
    )
```

### 5.2 Fact Tables

#### `fct_orders`

| Attribute | Value |
|---|---|
| **Primary Key** | `order_id` |
| **Foreign Keys** | `customer_id` → `dim_customers.customer_id` |
| **Grain** | One row per order |
| **Source** | `workspace.silver.silver_orders` INNER JOIN `workspace.gold.dim_customers` |

#### `fct_order_items`

| Attribute | Value |
|---|---|
| **Primary Key** | `order_id` + `order_item_id` (composite) |
| **Foreign Keys** | `order_id` → `fct_orders.order_id` \| `product_id` → `dim_products.product_id` |
| **Grain** | One row per order line item |
| **Source** | `workspace.silver.silver_order_items` INNER JOIN `workspace.gold.dim_products` |

> ⚠️ **Required:** `fct_orders` deve fazer join com `dim_customers` e `fct_order_items` deve fazer join com `dim_products`. Omitir o join de dimensão foi a causa raiz de atributos faltantes em execuções anteriores do pipeline.

### Python Template — `fct_orders`

```python
import dlt
from pyspark.sql.functions import current_timestamp

@dlt.table(
    name="fct_orders",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "orders",
        "pipelines.autoOptimize.zOrderCols": "order_id",
        "delta.enableChangeDataFeed": "false",
    },
)
def fct_orders():
    orders = dlt.read("workspace.silver.silver_orders")
    customers = dlt.read("workspace.gold.dim_customers")

    return (
        orders.join(customers, on="customer_id", how="inner")
        .select(
            orders["order_id"],
            orders["customer_id"],
            orders["order_status"],
            orders["order_purchase_timestamp"],
            orders["order_approved_at"],
            orders["order_delivered_carrier_date"],
            orders["order_delivered_customer_date"],
            orders["order_estimated_delivery_date"],
            # Derived
            orders["delivery_delay_days"],
            orders["order_processing_days"],
            orders["is_late_delivery"],
            # Customer dimension attributes
            customers["customer_unique_id"],
            customers["customer_city"],
            customers["customer_state"],
            customers["customer_location"],
        )
        .withColumn("_fact_processing_timestamp", current_timestamp())
    )
```

### Python Template — `fct_order_items`

```python
import dlt
from pyspark.sql.functions import current_timestamp

@dlt.table(
    name="fct_order_items",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "order_items",
        "pipelines.autoOptimize.zOrderCols": "order_id",
        "delta.enableChangeDataFeed": "false",
    },
)
def fct_order_items():
    items = dlt.read("workspace.silver.silver_order_items")
    products = dlt.read("workspace.gold.dim_products")

    return (
        items.join(products, on="product_id", how="inner")
        .select(
            items["order_id"],
            items["order_item_id"],
            items["product_id"],
            items["seller_id"],
            items["shipping_limit_date"],
            items["price"],
            items["freight_value"],
            items["total_item_value"],
            # Product dimension attributes
            products["product_category_name"],
            products["product_category_name_english"],
            products["product_volume_cm3"],
            products["product_weight_g"],
        )
        .withColumn("_fact_processing_timestamp", current_timestamp())
    )
```

---

## 6. Technical Configuration

### 6.1 Table Properties

> ℹ️ `delta.enableChangeDataFeed` deve ser `"false"` (ou omitido) no Gold — materialized views não suportam CDF.

| Property | Bronze | Silver | Gold |
|---|---|---|---|
| `quality` | `"bronze"` | `"silver"` | `"gold"` |
| `layer` | `"bronze"` | `"silver"` | `"gold"` |
| `domain` | per entity | per entity | per entity |
| `pipelines.autoOptimize.zOrderCols` | PK | PK | PK |
| `delta.enableChangeDataFeed` | `"true"` | `"true"` | `"false"` |

### 6.2 Metadata Fields

#### Bronze

| Field | Expression PySpark |
|---|---|
| `_ingest_timestamp` | `current_timestamp()` |
| `_source_file` | `col("_metadata.file_path")` |

#### Silver

| Field | Expression PySpark | Note |
|---|---|---|
| `_processing_timestamp` | `current_timestamp()` | Added in Silver transform |
| `_ingest_timestamp` | Preserved from Bronze | Carried forward as-is |

#### Gold

| Field | Expression PySpark | Scope |
|---|---|---|
| `_dimension_refresh_timestamp` | `current_timestamp()` | Dimensions only |
| `_fact_processing_timestamp` | `current_timestamp()` | Fact tables only |

### 6.3 `dlt.read()` vs `dlt.read_stream()`

| Context | Method | Motivo |
|---|---|---|
| Bronze → Silver (streaming) | `dlt.read_stream("catalog.schema.table")` | Ingestão incremental via Auto Loader |
| Silver → Gold (batch/MV) | `dlt.read("catalog.schema.table")` | Materialized Views usam leitura batch |
| Preprocessing view → `apply_changes` | `dlt.read_stream("view_name")` | CDC requer streaming |

---

## 7. Pipeline Configuration — Declarative Automation Bundles (DABs)

> ℹ️ **Updated name:** O produto antes chamado de *Databricks Asset Bundles* agora se chama **Declarative Automation Bundles (DABs)**. A sintaxe YAML não mudou.

> ⚠️ **Critical — target schema:** O campo `target` deve apontar para o schema de negócio do pipeline, **não** `observability`. O schema `observability` é reservado para logs de eventos do pipeline.

> ⚠️ **Critical — property placement:** `continuous` é uma propriedade de deploy e deve estar no bloco de recursos do pipeline. O bloco `configuration` aceita apenas pares chave-valor string para Spark/runtime.

### 7.1 File Structure

```
olist_ecommerce/
├── databricks.yml    ← bundle, workspace, resources e targets — tudo aqui
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
    │   ├── silver_customers.py        ← preprocessing view + create_streaming_table + apply_changes
    │   ├── silver_products.py
    │   └── silver_product_category.py
    └── gold/
        ├── gold_dim_customers.py
        ├── gold_dim_products.py
        ├── gold_fct_orders.py
        └── gold_fct_order_items.py
```

### 7.2 `databricks.yml` — Single File

```yaml
bundle:
  name: olist_ecommerce

workspace:
  host: https://<your-workspace>.cloud.databricks.com

resources:
  pipelines:
    olist_ecommerce_pipeline:
      name: olist_ecommerce_dev_lakeflow
      serverless: true
      continuous: false           # deploy property — NOT inside configuration:
      catalog: workspace
      target: ecommerce_analytics # business schema — NOT "observability"
      configuration:
        pipelines.enableTrackHistory: "true"
      libraries:
        - file:
            path: src/bronze/bronze_orders.py
        - file:
            path: src/bronze/bronze_order_items.py
        - file:
            path: src/bronze/bronze_customers.py
        - file:
            path: src/bronze/bronze_products.py
        - file:
            path: src/bronze/bronze_product_category.py
        - file:
            path: src/silver/silver_orders.py
        - file:
            path: src/silver/silver_order_items.py
        - file:
            path: src/silver/silver_customers.py
        - file:
            path: src/silver/silver_products.py
        - file:
            path: src/silver/silver_product_category.py
        - file:
            path: src/gold/gold_dim_customers.py
        - file:
            path: src/gold/gold_dim_products.py
        - file:
            path: src/gold/gold_fct_orders.py
        - file:
            path: src/gold/gold_fct_order_items.py

targets:
  dev:
    mode: development   # adds [dev <username>] prefix, pauses schedules automatically
    default: true

  prod:
    mode: production    # requires run_as
    run_as:
      service_principal_name: <service-principal-name>
```

> ℹ️ Este spec não requer um recurso de job separado. O pipeline pode ser acionado manualmente ou por um orquestrador externo quando implantado.

> ⚠️ `mode: development` faz o DABs adicionar automaticamente o prefixo `[dev <username>]` ao nome do pipeline, pausar qualquer schedule definido e isolar o deploy do ambiente de produção. `mode: production` exige que `run_as` seja definido.

> ℹ️ **Alternativa glob para libraries:** Em vez de listar cada arquivo Python individualmente, você pode usar:
> ```yaml
>       libraries:
>         - glob: src/**
> ```
> O campo `glob` **não pode ser usado junto com** `file` ou `notebook` no mesmo bloco `libraries`.

---

## 8. Table Naming Convention

> ⚠️ Não use o prefixo `LIVE.` em nenhum lugar. Essa sintaxe pertence ao modo de publicação legado (pré-fevereiro de 2025) e está depreciada. Todos os pipelines criados após 5 de fevereiro de 2025 usam o novo modo de publicação, que ignora silenciosamente `LIVE.` — causando erros de resolução difíceis de depurar.

| Context | Correct Reference | Incorrect / Legacy |
|---|---|---|
| Lendo Bronze a partir do Silver | `dlt.read_stream("workspace.bronze.bronze_orders")` | ~~`dlt.read_stream("LIVE.bronze_orders")`~~ |
| Join de dim no Gold fact | `dlt.read("workspace.gold.dim_customers")` | ~~`dlt.read("LIVE.dim_customers")`~~ |
| Notebook externo lendo Gold | `spark.table("workspace.gold.fct_orders")` | ~~`spark.table("fct_orders")`~~ (ambíguo) |
| Preprocessing view no apply_changes | `dlt.read_stream("silver_customers_preprocessed")` (nome simples) | ~~`dlt.read_stream("workspace.silver.silver_customers_preprocessed")`~~ |

---

## 9. Expected Output Structure

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
    │   ├── silver_customers.py        ← preprocessing view + create_streaming_table + apply_changes
    │   ├── silver_products.py
    │   └── silver_product_category.py
    └── gold/
        ├── gold_dim_customers.py
        ├── gold_dim_products.py
        ├── gold_fct_orders.py         ← INNER JOIN em dim_customers obrigatório
        └── gold_fct_order_items.py    ← INNER JOIN em dim_products obrigatório
```

---

## 10. Validation Requirements

| # | Requirement | Details |
|---|---|---|
| 1 | Full 3-part table names | Todas as tabelas usam `catalog.schema.table`. Sem prefixo `LIVE.` em nenhum lugar. |
| 2 | SCD Type 2 — três chamadas separadas | `silver_customers.py` precisa de `@dlt.view` (preprocessing) + `dlt.create_streaming_table()` + `dlt.apply_changes()`. |
| 3 | `except_column_list` e `track_history_except_column_list` são independentes | Ambos os parâmetros devem aparecer na chamada `apply_changes()` e não são intercambiáveis. |
| 4 | `fct_orders` faz join com `dim_customers` | `gold_fct_orders.py` deve usar `.join(dim_customers, on="customer_id", how="inner")`. |
| 5 | `fct_order_items` faz join com `dim_products` | `gold_fct_order_items.py` deve usar `.join(dim_products, on="product_id", how="inner")`. |
| 6 | Target schema é `ecommerce_analytics` | O campo `target` no DABs deve ser `ecommerce_analytics`, não `observability`. |
| 7 | `continuous` no bloco de recursos | `continuous: false` vai em `resources.pipelines.<name>`, nunca dentro de `configuration:`. |
| 8 | Nenhum recurso de job necessário | O spec valida um bundle apenas com pipeline; agendamento externo ou trigger manual é aceitável. |
| 9 | CDF desabilitado no Gold | `delta.enableChangeDataFeed` deve ser `"false"` (ou omitido) para as tabelas Gold. |
| 10 | Auto Loader via `cloudFiles` em Python | Tabelas Bronze devem usar `spark.readStream.format("cloudFiles")` com `cloudFiles.format`, `cloudFiles.schemaLocation` declarado explicitamente. Nunca use `read_files()` ou a sintaxe SQL em Python. |
| 11 | `dlt.read()` vs `dlt.read_stream()` | Use `dlt.read_stream()` para streaming (Bronze → Silver). Use `dlt.read()` para leituras batch no Gold. |
| 12 | `databricks.yml` com `bundle`, `workspace`, `targets` e pipeline resource | O arquivo raiz deve declarar esses blocos; um recurso de job separado é opcional. |
| 13 | `targets` com `mode: development` e `mode: production` | Use `mode:` em vez de `development: true` (campo legado). `mode: development` pausa schedules automaticamente. `mode: production` requer `run_as`. |
| 14 | Nunca usar `schema=` com valor `catalog.schema` em decoradores DLT | O parâmetro `schema` em `@dlt.table()` e `dlt.create_streaming_table()` é para DDL de colunas, **não** localização de publicação. Usar `schema="workspace.bronze"` causa erro de sintaxe SQL. Remover o parâmetro; a localização é definida por `catalog` e `target` no `databricks.yml`. |
| 15 | `cloudFiles.schemaLocation` deve apontar para o Unity Catalog Volume | O acesso ao DBFS root público (`/tmp/`, `/dbfs/`, `/FileStore/`) está desabilitado neste workspace. Usar `/tmp/schema/<entity>` causa erro de permissão. Sempre use `/Volumes/workspace/default/ecommerce_raw_volume/_schemas/<entity>` como `schemaLocation` para todos os arquivos Bronze. |
| 16 | Paths do Auto Loader devem apontar para diretórios, não arquivos | No volume deste workspace, cada dataset é armazenado **dentro de um diretório** — inclusive diretórios cujo nome termina em `.csv` (e.g., `olist_products_dataset.csv/`). O Auto Loader deve receber o caminho do **diretório** (com `/` final). Passar um caminho de arquivo causa `FileNotFoundException`. A única exceção é `product_category/`, cujo diretório não carrega extensão `.csv`. |
