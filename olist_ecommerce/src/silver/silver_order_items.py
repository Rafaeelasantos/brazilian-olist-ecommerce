import dlt
from pyspark.sql.functions import col, round as spark_round


@dlt.table(
    name="silver_order_items",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "order_items",
        "pipelines.autoOptimize.zOrderCols": "order_id",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Silver layer — cleaned order items with derived total item value",
)
@dlt.expect("order_id is not null", "order_id IS NOT NULL")
@dlt.expect("product_id is not null", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_price", "price IS NOT NULL AND CAST(price AS DOUBLE) >= 0")
def silver_order_items():
    return (
        dlt.read_stream("bronze_order_items")
        .select(
            col("order_id").cast("string"),
            col("order_item_id").cast("integer"),
            col("product_id").cast("string"),
            col("seller_id").cast("string"),
            col("shipping_limit_date").cast("string"),
            col("price").cast("double"),
            col("freight_value").cast("double"),
            col("_ingest_timestamp"),
            col("_source_file"),
        )
        .withColumn(
            "total_item_value",
            spark_round(col("price") + col("freight_value"), 2),
        )
    )
