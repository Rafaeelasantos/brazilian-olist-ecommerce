import dlt
from pyspark.sql.functions import col, current_timestamp


@dlt.table(
    name="dim_customers",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "customers",
        "pipelines.autoOptimize.zOrderCols": "customer_id",
        "delta.enableChangeDataFeed": "false",
    },
    comment="Camada Gold - dimensao de customers (snapshot ativo do SCD Type 2)",
)
def dim_customers():
    return (
        dlt.read_stream("silver_customers")
        .filter("__END_AT IS NULL")
        .select(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_zip_code_prefix"),
            col("customer_city"),
            col("customer_state"),
            col("customer_location"),
            current_timestamp().alias("_dimension_refresh_timestamp"),
        )
    )
