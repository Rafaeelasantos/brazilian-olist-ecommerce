import dlt
from pyspark.sql.functions import col, current_timestamp


@dlt.table(
    name="dim_customers",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "customers",
        "pipelines.autoOptimize.zOrderCols": "customer_id",
    },
    comment="Gold layer — customer dimension (current record only, active SCD Type 2 rows)",
)
def dim_customers():
    return (
        dlt.read("silver_customers")
        .filter(col("__END_AT").isNull())
        .select(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_zip_code_prefix"),
            col("customer_city"),
            col("customer_state"),
            col("customer_location"),
            current_timestamp().alias("_gold_timestamp"),
        )
    )
