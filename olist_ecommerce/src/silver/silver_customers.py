import dlt
from pyspark.sql.functions import current_timestamp, concat, col, lit


# Step 1: Preprocessing view with quality constraints and derived fields
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


# Step 2: Declare the target streaming table (required before apply_changes)
dlt.create_streaming_table(
    name="silver_customers",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "customers",
        "pipelines.autoOptimize.zOrderCols": "customer_id",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Silver layer — SCD Type 2 customers dimension",
)


# Step 3: Apply CDC with SCD Type 2
dlt.apply_changes(
    target="workspace.silver.silver_customers",
    source="silver_customers_preprocessed",
    keys=["customer_id"],
    sequence_by=col("_ingest_timestamp"),
    stored_as_scd_type=2,
    except_column_list=["_processing_timestamp", "_ingest_timestamp"],
    track_history_except_column_list=["_processing_timestamp", "_ingest_timestamp"],
)
