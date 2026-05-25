import dlt
from pyspark.sql.functions import current_timestamp, datediff, col, when


@dlt.table(
    name="silver_orders",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "orders",
        "pipelines.autoOptimize.zOrderCols": "order_id",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Silver layer — cleaned orders with derived delivery metrics",
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_status", "order_status IS NOT NULL")
def silver_orders():
    delay = datediff(
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date"),
    ).alias("delivery_delay_days")

    return (
        dlt.read_stream("workspace.bronze.bronze_orders")
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "_ingest_timestamp",
            delay,
            datediff(col("order_approved_at"), col("order_purchase_timestamp")).alias(
                "order_processing_days"
            ),
            current_timestamp().alias("_processing_timestamp"),
        )
        .withColumn(
            "is_late_delivery",
            when(col("delivery_delay_days") > 0, True).otherwise(False),
        )
    )
