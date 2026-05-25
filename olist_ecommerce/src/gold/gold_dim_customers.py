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
    comment="Gold layer — current customer dimension (SCD2 active rows only)",
)
def dim_customers():
    return (
        dlt.read("workspace.silver.silver_customers")
        .filter("__END_AT IS NULL")  # SCD2 — current rows only
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
