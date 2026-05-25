import dlt
from pyspark.sql.functions import current_timestamp, col


@dlt.table(
    name="silver_order_items",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "order_items",
        "pipelines.autoOptimize.zOrderCols": "order_id",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Silver layer — cleaned order items with derived total value",
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_price", "price >= 0")
def silver_order_items():
    return (
        dlt.read_stream("workspace.bronze.bronze_order_items")
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
            "_ingest_timestamp",
            (col("price") + col("freight_value")).alias("total_item_value"),
            current_timestamp().alias("_processing_timestamp"),
        )
    )
