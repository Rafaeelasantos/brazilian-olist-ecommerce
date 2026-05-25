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

> 📐 **Code templates:** All PySpark / DLT code patterns are defined in `.cursor/rules/pipeline_templates.mdc`. Follow those templates exactly when generating or reviewing pipeline files.

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

> ℹ️ `_metadata.file_path` é uma coluna de metadados exposta pelo Auto Loader. Ela deve ser referenciada via `col("_metadata.file_path")` antes de qualquer transformação que remova colunas de metadados.

> 📐 **Template:** Use **Template 1** from `.cursor/rules/pipeline_templates.mdc` for all Bronze tables.

> ⚠️ **Critical — `schema` parameter in `@dlt.table()` / `dlt.create_streaming_table()`:** O parâmetro `schema` nessas funções é reservado para **definição de colunas DDL** (ex.: `"order_id STRING, price DOUBLE"`). Ele **não** define onde a tabela é publicada. Passar `schema="workspace.bronze"` (ou qualquer valor `catalog.schema`) faz o runtime tentar interpretar a string como DDL de colunas, causando um erro de sintaxe SQL. A localização de publicação (catalog + schema) é controlada pelos campos `catalog` e `target` no `databricks.yml`. **Nunca passe `schema=` com valores `catalog.schema` nos decoradores DLT.**

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

| Constraint | Expression |
|---|---|
| `valid_order_id` | `order_id IS NOT NULL` |
| `valid_customer_id` | `customer_id IS NOT NULL` |
| `valid_status` | `order_status IS NOT NULL` |

> 📐 **Template:** Use **Template 2** from `.cursor/rules/pipeline_templates.mdc` for `silver_orders`.

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

| Constraint | Expression |
|---|---|
| `valid_order_id` | `order_id IS NOT NULL` |
| `valid_product_id` | `product_id IS NOT NULL` |
| `valid_price` | `price >= 0` |

> 📐 **Template:** Use **Template 2** from `.cursor/rules/pipeline_templates.mdc` for `silver_order_items`.

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

| Constraint | Expression |
|---|---|
| `valid_customer_id` | `customer_id IS NOT NULL` |
| `valid_unique_id` | `customer_unique_id IS NOT NULL` |

> 📐 **Template:** Use **Template 3** from `.cursor/rules/pipeline_templates.mdc` for `silver_customers`.

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

| Constraint | Expression |
|---|---|
| `valid_product_id` | `product_id IS NOT NULL` |
| `valid_weight` | `product_weight_g > 0` |

> 📐 **Template:** Use **Template 2** from `.cursor/rules/pipeline_templates.mdc` for `silver_products`.

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

> 📐 **Template:** Use **Template 2** from `.cursor/rules/pipeline_templates.mdc` for `silver_product_category`.

---

## 4. Silver Layer — SCD Type 2 (Customers)

> ⚠️ **Critical:** Em PySpark, o SCD Type 2 é implementado com `dlt.apply_changes()`. Isso requer **três definições separadas**:
> 1. `@dlt.view` — preprocessing view para filtros e derived fields.
> 2. `dlt.create_streaming_table()` — declara a tabela alvo.
> 3. `dlt.apply_changes()` — aplica as mudanças CDC.
>
> Todas no mesmo arquivo Python, mas são chamadas independentes — não decoradores aninhados.

> ℹ️ `apply_changes()` não aceita filtros diretamente. Se precisar de filtragem ou validação de qualidade, crie uma view de pré-processamento com `@dlt.view` antes de chamar `apply_changes`.

> 📐 **Template:** Use **Template 3** from `.cursor/rules/pipeline_templates.mdc` for `silver_customers.py`.

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

> ℹ️ `except_column_list` controla quais campos da fonte são escritos no target. `track_history_except_column_list` controla quais campos, quando alterados, disparam uma nova linha SCD2. São parâmetros independentes e ambos devem ser declarados.

---

## 5. Gold Layer — Star Schema

### 5.1 Dimensions

| Dimension | Source | Filter | Join Key to Fact |
|---|---|---|---|
| `dim_customers` | `workspace.silver.silver_customers` | `__END_AT IS NULL` (SCD2 current rows) | `customer_id` |
| `dim_products` | `workspace.silver.silver_products` | none | `product_id` |

> 📐 **Template:** Use **Template 4** from `.cursor/rules/pipeline_templates.mdc` for all Gold dimension tables.

### 5.2 Facts

| Fact | Source | Grain | Dimension Join |
|---|---|---|---|
| `fct_orders` | `workspace.silver.silver_orders` | One row per order | `INNER JOIN dim_customers ON customer_id` |
| `fct_order_items` | `workspace.silver.silver_order_items` | One row per order item | `INNER JOIN dim_products ON product_id` |

> ⚠️ **Critical:** Both fact tables **must** perform their INNER JOIN with the respective dimension. Omitting the join causes missing dimension attributes in reporting.

> 📐 **Template:** Use **Template 5** from `.cursor/rules/pipeline_templates.mdc` for all Gold fact tables.

#### `fct_orders` — Fields

| Field | Source |
|---|---|
| `order_id` | `silver_orders` (PK) |
| `customer_id` | `silver_orders` (FK) |
| `order_status` | `silver_orders` |
| `order_purchase_timestamp` | `silver_orders` |
| `order_approved_at` | `silver_orders` |
| `order_delivered_carrier_date` | `silver_orders` |
| `order_delivered_customer_date` | `silver_orders` |
| `order_estimated_delivery_date` | `silver_orders` |
| `delivery_delay_days` | `silver_orders` (derived) |
| `order_processing_days` | `silver_orders` (derived) |
| `is_late_delivery` | `silver_orders` (derived) |
| `customer_unique_id` | `dim_customers` |
| `customer_city` | `dim_customers` |
| `customer_state` | `dim_customers` |
| `customer_location` | `dim_customers` (derived) |
| `_fact_processing_timestamp` | `current_timestamp()` |

#### `fct_order_items` — Fields

| Field | Source |
|---|---|
| `order_id` | `silver_order_items` (FK) |
| `order_item_id` | `silver_order_items` |
| `product_id` | `silver_order_items` (FK) |
| `seller_id` | `silver_order_items` |
| `shipping_limit_date` | `silver_order_items` |
| `price` | `silver_order_items` |
| `freight_value` | `silver_order_items` |
| `total_item_value` | `silver_order_items` (derived) |
| `product_category_name` | `dim_products` |
| `product_category_name_english` | `dim_products` |
| `product_volume_cm3` | `dim_products` (derived) |
| `product_weight_g` | `dim_products` |
| `_fact_processing_timestamp` | `current_timestamp()` |

---

## 6. Table Properties

| Property | Bronze | Silver | Gold |
|---|---|---|---|
| `quality` | `"bronze"` | `"silver"` | `"gold"` |
| `layer` | `"bronze"` | `"silver"` | `"gold"` |
| `domain` | entity name | entity name | entity name |
| `pipelines.autoOptimize.zOrderCols` | PK field | PK field | PK field |
| `delta.enableChangeDataFeed` | `"true"` | `"true"` | `"false"` |

---

## 7. Metadata Fields

| Field | Layer | Logic |
|---|---|---|
| `_ingest_timestamp` | Bronze | `current_timestamp()` at ingestion |
| `_source_file` | Bronze | `col("_metadata.file_path")` |
| `_processing_timestamp` | Silver | `current_timestamp()` at transformation |
| `_dimension_refresh_timestamp` | Gold (dim) | `current_timestamp()` when dimension is refreshed |
| `_fact_processing_timestamp` | Gold (fact) | `current_timestamp()` when fact is processed |

---

## 8. Databricks Asset Bundle (`databricks.yml`)

| Field | Value |
|---|---|
| `bundle.name` | `olist_ecommerce` |
| `workspace.host` | `https://dbc-f76716c3-b252.cloud.databricks.com/` |
| `targets.dev.mode` | `development` |
| `pipeline.target` | `ecommerce_analytics` |
| `pipeline.continuous` | `false` (in `resources.pipelines.<name>`, not inside `configuration:`) |

> ⚠️ `continuous: false` goes in the `resources.pipelines.<name>` block, **never** inside `configuration:`.  
> ⚠️ `mode: production` requires `run_as` to be declared.  
> ⚠️ Never pass `schema="catalog.schema"` to `@dlt.table()` — the publication location is controlled by `catalog` and `target` in `databricks.yml`.

---

> **Version:** 2.0.0  
> **Updated:** 2026-05-25
