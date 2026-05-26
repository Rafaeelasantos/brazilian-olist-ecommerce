<<<<<<< HEAD
# Olist E-Commerce Analytics — Lakeflow Pipeline

**Version:** 2.0 | **Environment:** dev | **Platform:** Databricks Lakeflow (DLT)

---

## Overview

This project implements a production-ready **Medallion Architecture** (Bronze → Silver → Gold) for the Olist Brazilian E-Commerce dataset using **Databricks Lakeflow Pipelines** (Delta Live Tables) with Unity Catalog.

| Field | Value |
|---|---|
| **Catalog** | `workspace` |
| **Bronze Schema** | `workspace.bronze` |
| **Silver Schema** | `workspace.silver` |
| **Gold Schema** | `workspace.gold` |
| **Pipeline Target Schema** | `ecommerce_analytics` |
| **Pipeline Name** | `olist_ecommerce_dev_lakeflow` |
| **Workspace** | `https://dbc-f76716c3-b252.cloud.databricks.com/` |

---
=======
# Olist E-Commerce — Databricks Asset Bundle (DLT Pipeline)

Medallion architecture pipeline built with **Databricks Delta Live Tables (DLT)** and deployed via **Databricks Asset Bundles (DAB)**.
>>>>>>> rescue-sdd

## Architecture

```
<<<<<<< HEAD
Raw CSV Files (Unity Catalog Volume)
        │
        ▼
🥉 BRONZE — Auto Loader (cloudFiles)
   Immutable raw ingestion + metadata fields
        │
        ▼
🥈 SILVER — Curated Layer
   Type casting, derived fields, DQ constraints
   SCD Type 2 for Customers (apply_changes)
        │
        ▼
🥇 GOLD — Star Schema
   Dimensions + Facts ready for reporting
```

=======
Bronze  →  Silver  →  Gold
(raw)      (clean)    (aggregated / dimensional)
```

### Bronze Layer — Auto Loader (Template 1)
Ingests raw CSV files from Unity Catalog Volumes using Auto Loader (`cloudFiles`). All columns are read as strings with schema evolution disabled. Metadata columns `_ingest_timestamp` and `_source_file` are appended.

| Table | Source File |
|---|---|
| `bronze_customers` | `olist_customers_dataset.csv` |
| `bronze_orders` | `olist_orders_dataset.csv` |
| `bronze_order_items` | `olist_order_items_dataset.csv` |
| `bronze_products` | `olist_products_dataset.csv` |
| `bronze_product_category` | `product_category/` |

### Silver Layer — Cleansed & Typed (Templates 2 & 3)
Casts columns to correct types, applies data quality expectations (`@dlt.expect_or_drop`), and enriches records with derived columns.

| Table | Template | Key Derivations |
|---|---|---|
| `silver_orders` | 2 — Streaming | `delivery_delay_days`, `order_processing_days`, `is_late_delivery` |
| `silver_order_items` | 2 — Streaming | `total_item_value` (price + freight) |
| `silver_customers` | 3 — SCD Type 2 | `customer_location` (city + state) |
| `silver_products` | 2 — Streaming + join | `product_volume_cm3`, `product_category_name_english` |
| `silver_product_category` | 2 — Streaming | lowercase/trimmed category names |

### Gold Layer — Dimensional Model (Templates 4 & 5)
Business-ready tables built on top of silver, joining facts with dimensions.

| Table | Type | Description |
|---|---|---|
| `dim_customers` | Dimension | Active customer records (SCD2 `__END_AT IS NULL`) |
| `dim_products` | Dimension | Products with English category name and volume |
| `fct_orders` | Fact | Orders enriched with customer dimension |
| `fct_order_items` | Fact | Line items enriched with order, customer, and product context |

>>>>>>> rescue-sdd
---

## Project Structure

```
olist_ecommerce/
<<<<<<< HEAD
├── databricks.yml              ← Databricks Asset Bundle configuration
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
    │   ├── silver_customers.py        ← SCD Type 2
    │   ├── silver_products.py
    │   └── silver_product_category.py
    └── gold/
        ├── gold_dim_customers.py
        ├── gold_dim_products.py
        ├── gold_fct_orders.py
        └── gold_fct_order_items.py
=======
├── databricks.yml                    # DAB bundle configuration
├── resources/
│   └── olist_ecommerce_pipeline.yml  # DLT pipeline resource definition
├── src/
│   ├── bronze/
│   │   ├── bronze_customers.py
│   │   ├── bronze_orders.py
│   │   ├── bronze_order_items.py
│   │   ├── bronze_products.py
│   │   └── bronze_product_category.py
│   ├── silver/
│   │   ├── silver_customers.py
│   │   ├── silver_orders.py
│   │   ├── silver_order_items.py
│   │   ├── silver_products.py
│   │   └── silver_product_category.py
│   └── gold/
│       ├── dim_customers.py
│       ├── dim_products.py
│       ├── fct_orders.py
│       └── fct_order_items.py
└── README.md
>>>>>>> rescue-sdd
```

---

<<<<<<< HEAD
## Data Sources

| Entity | Volume Path |
|---|---|
| `orders` | `/Volumes/workspace/default/ecommerce_raw_volume/olist_orders_dataset.csv/` |
| `order_items` | `/Volumes/workspace/default/ecommerce_raw_volume/olist_order_items_dataset.csv/` |
| `customers` | `/Volumes/workspace/default/ecommerce_raw_volume/olist_customers_dataset.csv/` |
| `products` | `/Volumes/workspace/default/ecommerce_raw_volume/olist_products_dataset.csv/` |
| `product_category` | `/Volumes/workspace/default/ecommerce_raw_volume/product_category/` |

---

## Tables

### Bronze Layer

| Table | Description |
|---|---|
| `workspace.bronze.bronze_orders` | Raw orders |
| `workspace.bronze.bronze_order_items` | Raw order items |
| `workspace.bronze.bronze_customers` | Raw customers |
| `workspace.bronze.bronze_products` | Raw products |
| `workspace.bronze.bronze_product_category` | Raw product category translations |

### Silver Layer

| Table | Description |
|---|---|
| `workspace.silver.silver_orders` | Cleaned orders with derived fields |
| `workspace.silver.silver_order_items` | Cleaned order items with derived fields |
| `workspace.silver.silver_customers` | SCD Type 2 customers dimension |
| `workspace.silver.silver_products` | Cleaned products enriched with English category name |
| `workspace.silver.silver_product_category` | Product category lookup table |

### Gold Layer (Star Schema)

| Table | Type | Description |
|---|---|---|
| `workspace.gold.dim_customers` | Dimension | Current customer records (SCD2 `__END_AT IS NULL`) |
| `workspace.gold.dim_products` | Dimension | Product dimension |
| `workspace.gold.fct_orders` | Fact | One row per order, enriched with customer attributes |
| `workspace.gold.fct_order_items` | Fact | One row per order item, enriched with product attributes |

---

## Deployment

### Prerequisites

- Databricks CLI installed and configured
- Profile: `rafaela.aws1992@gmail.com`

### Commands

```bash
# Validate bundle configuration
databricks bundle validate --profile rafaela.aws1992@gmail.com

# Deploy to dev
databricks bundle deploy --profile rafaela.aws1992@gmail.com -t dev

# Run pipeline manually
databricks bundle run olist_ecommerce_dev_lakeflow -t dev

# Deploy to production
databricks bundle deploy --profile rafaela.aws1992@gmail.com -t prod
=======
## Deployment

### Prerequisites
- Databricks CLI ≥ 0.220 authenticated (`databricks auth login`)
- Unity Catalog enabled workspace
- Volume: `/Volumes/workspace/default/ecommerce_raw_volume/` with raw CSV files

### Validate bundle
```bash
cd olist_ecommerce
databricks bundle validate
```

### Deploy (dev)
```bash
databricks bundle deploy --target dev
```

### Deploy (prod)
```bash
databricks bundle deploy --target prod
```

### Run pipeline
```bash
databricks bundle run olist_ecommerce_pipeline --target dev
>>>>>>> rescue-sdd
```

---

<<<<<<< HEAD
## Key Design Decisions

- **No `LIVE.` prefix** — Unity Catalog 3-part names used everywhere (`catalog.schema.table`)
- **SCD Type 2** — Implemented via `dlt.apply_changes()` for the customers dimension
- **Auto Loader** — `spark.readStream.format("cloudFiles")` for all Bronze tables
- **CDF disabled on Gold** — `delta.enableChangeDataFeed = "false"` on all Gold tables
- **Schema inference disabled** — `inferSchema=false` for production stability
- **Schema location** — Always on Unity Catalog Volume, never `/tmp/`
=======
## Data Quality

All silver tables enforce quality rules via `@dlt.expect_or_drop` — rows violating constraints are dropped and tracked in DLT event logs:

| Table | Constraint | Rule |
|---|---|---|
| `silver_orders` | `valid_order_id` | `order_id IS NOT NULL` |
| `silver_orders` | `valid_customer_id` | `customer_id IS NOT NULL` |
| `silver_orders` | `valid_status` | `order_status IS NOT NULL` |
| `silver_order_items` | `valid_order_id` | `order_id IS NOT NULL` |
| `silver_order_items` | `valid_product_id` | `product_id IS NOT NULL` |
| `silver_order_items` | `valid_price` | `price IS NOT NULL AND price >= 0` |
| `silver_customers` | `valid_customer_id` | `customer_id IS NOT NULL` |
| `silver_products` | `valid_product_id` | `product_id IS NOT NULL` |
| `silver_product_category` | `valid_category_name` | `product_category_name IS NOT NULL` |

---

## Metadata Columns

| Column | Layer | Description |
|---|---|---|
| `_ingest_timestamp` | Bronze → Silver | Timestamp when the record was loaded by Auto Loader |
| `_source_file` | Bronze → Silver | Source file path from `_metadata.file_path` |
| `_processing_timestamp` | Silver | Timestamp when the silver transformation ran |
| `_gold_timestamp` | Gold | Timestamp when the gold table was written |
>>>>>>> rescue-sdd
