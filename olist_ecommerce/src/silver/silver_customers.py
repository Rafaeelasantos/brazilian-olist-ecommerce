import dlt
from pyspark.sql.functions import col, upper, trim, current_timestamp, concat_ws


# ── 1. Staging view: clean raw bronze data ──────────────────────────────────
@dlt.view(
    name="silver_customers_preprocessed",
    comment="Staging view — cleaned bronze customers ready for SCD Type 2 apply_changes",
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_unique_id", "customer_unique_id IS NOT NULL")
def silver_customers_preprocessed():
    return (
        dlt.read_stream("bronze_customers")
        .select(
            col("customer_id").cast("string"),
            col("customer_unique_id").cast("string"),
            col("customer_zip_code_prefix").cast("string"),
            trim(upper(col("customer_city"))).alias("customer_city"),
            trim(upper(col("customer_state"))).alias("customer_state"),
            col("_ingest_timestamp"),
            col("_source_file"),
            current_timestamp().alias("_processing_timestamp"),
        )
        .withColumn(
            "customer_location",
            concat_ws(", ", col("customer_city"), col("customer_state")),
        )
    )


# ── 2. Target SCD Type 2 table definition ───────────────────────────────────
dlt.create_streaming_table(
    name="silver_customers",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "customers",
        "pipelines.autoOptimize.zOrderCols": "customer_id",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Silver layer — SCD Type 2 customers tracking city and state changes",
)

# ── 3. Apply changes (SCD Type 2) ───────────────────────────────────────────
dlt.apply_changes(
    target="silver_customers",
    source="silver_customers_preprocessed",
    keys=["customer_id"],
    sequence_by=col("_ingest_timestamp"),
    stored_as_scd_type=2,
    except_column_list=["_ingest_timestamp", "_source_file", "_processing_timestamp"],
    track_history_except_column_list=["customer_unique_id"],
)
