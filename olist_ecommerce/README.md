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

## Architecture

```
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

---

## Project Structure

```
olist_ecommerce/
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
```

---

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
```

---

## Key Design Decisions

- **No `LIVE.` prefix** — Unity Catalog 3-part names used everywhere (`catalog.schema.table`)
- **SCD Type 2** — Implemented via `dlt.apply_changes()` for the customers dimension
- **Auto Loader** — `spark.readStream.format("cloudFiles")` for all Bronze tables
- **CDF disabled on Gold** — `delta.enableChangeDataFeed = "false"` on all Gold tables
- **Schema inference disabled** — `inferSchema=false` for production stability
- **Schema location** — Always on Unity Catalog Volume, never `/tmp/`
