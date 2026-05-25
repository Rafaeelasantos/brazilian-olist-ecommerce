# Olist E-Commerce — Databricks Asset Bundle (DLT Pipeline)

Medallion architecture pipeline for the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) using **Databricks Delta Live Tables (DLT)** and the **Databricks Asset Bundle (DAB)** framework.

---

## Architecture

```
Raw CSV (Volume)
      │
      ▼
┌─────────────┐
│   BRONZE    │  STREAMING TABLEs — raw ingestion via read_files()
│             │  Adds: _ingest_timestamp, _source_file
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   SILVER    │  MATERIALIZED VIEWs — cast types, trim strings, deduplicate
│             │  silver_customers uses APPLY CHANGES (SCD Type 2)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    GOLD     │  MATERIALIZED VIEWs — aggregated, business-ready tables
│             │  Optimised for BI / ML consumption
└─────────────┘
```

---

## Project Structure

```
olist_ecommerce/
├── databricks.yml                         # Bundle configuration
└── src/
    ├── bronze/
    │   ├── bronze_orders.sql
    │   ├── bronze_order_items.sql
    │   ├── bronze_customers.sql
    │   ├── bronze_products.sql
    │   ├── bronze_product_category.sql
    │   ├── bronze_sellers.sql
    │   ├── bronze_order_payments.sql
    │   └── bronze_order_reviews.sql
    ├── silver/
    │   ├── silver_orders.sql
    │   ├── silver_order_items.sql
    │   ├── silver_customers.sql           ← SCD Type 2
    │   ├── silver_products.sql
    │   ├── silver_product_category.sql
    │   ├── silver_sellers.sql
    │   ├── silver_order_payments.sql
    │   └── silver_order_reviews.sql
    └── gold/
        ├── gold_orders_summary.sql
        ├── gold_revenue_by_category.sql
        ├── gold_seller_performance.sql
        └── gold_customer_ltv.sql
```

---

## Layers

### Bronze
| Table | Source File | Key |
|---|---|---|
| `bronze_orders` | `olist_orders_dataset.csv` | `order_id` |
| `bronze_order_items` | `olist_order_items_dataset.csv` | `order_id, order_item_id` |
| `bronze_customers` | `olist_customers_dataset.csv` | `customer_id` |
| `bronze_products` | `olist_products_dataset.csv` | `product_id` |
| `bronze_product_category` | `product_category_name_translation.csv` | `product_category_name` |
| `bronze_sellers` | `olist_sellers_dataset.csv` | `seller_id` |
| `bronze_order_payments` | `olist_order_payments_dataset.csv` | `order_id, payment_sequential` |
| `bronze_order_reviews` | `olist_order_reviews_dataset.csv` | `review_id` |

### Silver
| Table | Pattern | Notes |
|---|---|---|
| `silver_orders` | Materialized View | Type casts, dedup by `order_id` |
| `silver_order_items` | Materialized View | Type casts, dedup by `order_id, order_item_id` |
| `silver_customers` | Streaming Table + APPLY CHANGES | **SCD Type 2** — full history tracked |
| `silver_products` | Materialized View | Normalises column name typos from source |
| `silver_product_category` | Materialized View | Portuguese → English name mapping |
| `silver_sellers` | Materialized View | Trim & dedup by `seller_id` |
| `silver_order_payments` | Materialized View | Type casts, dedup by `order_id, payment_sequential` |
| `silver_order_reviews` | Materialized View | Type casts, dedup by `review_id` |

### Gold
| Table | Description |
|---|---|
| `gold_orders_summary` | Per-order KPIs: items, revenue, payments, review score, delivery days |
| `gold_revenue_by_category` | Monthly revenue & GMV by product category (English name) |
| `gold_seller_performance` | Seller-level aggregations: revenue, delivery speed, review scores |
| `gold_customer_ltv` | Customer lifetime value, tenure, order history |

---

## Prerequisites

| Requirement | Detail |
|---|---|
| Databricks CLI | ≥ 0.200 with DAB support |
| Workspace | Unity Catalog enabled (`workspace` catalog) |
| Schemas | `workspace.bronze`, `workspace.silver`, `workspace.gold` must exist (or UC auto-create enabled) |
| Volume | `/Volumes/workspace/default/ecommerce_raw_volume/` containing the Olist CSV files |
| Cluster policy | DLT-compatible (Photon recommended) |

---

## Deployment

```bash
# Authenticate
databricks auth login --host https://<your-workspace>.azuredatabricks.net

# Validate bundle
databricks bundle validate

# Deploy pipeline (creates/updates DLT pipeline in Databricks)
databricks bundle deploy --target ecommerce_analytics

# Run pipeline
databricks bundle run ecommerce_dlt_pipeline --target ecommerce_analytics
```

---

## Data Quality

- **Bronze**: all rows ingested (schema-on-read); no constraints enforced.
- **Silver**: deduplication via `QUALIFY ROW_NUMBER() … = 1`; timestamp parsing and type casting surface errors early.
- **Silver Customers**: full change history via SCD Type 2 (`APPLY CHANGES INTO … STORED AS SCD TYPE 2`).
- **Gold**: business logic filters (e.g. `WHERE order_status = 'delivered'`) ensure only meaningful records are aggregated.
