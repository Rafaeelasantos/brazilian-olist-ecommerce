import dlt
<<<<<<< HEAD
from pyspark.sql.functions import col, to_timestamp, datediff, when
=======
from pyspark.sql.functions import col, to_timestamp, datediff, when, current_timestamp
>>>>>>> rescue-sdd


@dlt.table(
    name="silver_orders",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "orders",
        "pipelines.autoOptimize.zOrderCols": "order_id",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Silver layer — cleaned and typed orders with derived delivery metrics",
)
<<<<<<< HEAD
@dlt.expect("order_id is not null", "order_id IS NOT NULL")
@dlt.expect("customer_id is not null", "customer_id IS NOT NULL")
=======
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
>>>>>>> rescue-sdd
@dlt.expect_or_drop("valid_order_status", "order_status IS NOT NULL")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .select(
            col("order_id").cast("string"),
            col("customer_id").cast("string"),
            col("order_status").cast("string"),
            to_timestamp(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss").alias(
                "order_purchase_timestamp"
            ),
            to_timestamp(col("order_approved_at"), "yyyy-MM-dd HH:mm:ss").alias(
                "order_approved_at"
            ),
            to_timestamp(
                col("order_delivered_carrier_date"), "yyyy-MM-dd HH:mm:ss"
            ).alias("order_delivered_carrier_date"),
            to_timestamp(
                col("order_delivered_customer_date"), "yyyy-MM-dd HH:mm:ss"
            ).alias("order_delivered_customer_date"),
            to_timestamp(
                col("order_estimated_delivery_date"), "yyyy-MM-dd HH:mm:ss"
            ).alias("order_estimated_delivery_date"),
            col("_ingest_timestamp"),
            col("_source_file"),
<<<<<<< HEAD
        )
        .withColumn(
            "delivery_days",
            when(
                col("order_delivered_customer_date").isNotNull()
                & col("order_purchase_timestamp").isNotNull(),
                datediff(
                    col("order_delivered_customer_date"),
=======
            current_timestamp().alias("_processing_timestamp"),
        )
        .withColumn(
            "delivery_delay_days",
            when(
                col("order_delivered_customer_date").isNotNull()
                & col("order_estimated_delivery_date").isNotNull(),
                datediff(
                    col("order_delivered_customer_date"),
                    col("order_estimated_delivery_date"),
                ),
            ).otherwise(None),
        )
        .withColumn(
            "order_processing_days",
            when(
                col("order_approved_at").isNotNull()
                & col("order_purchase_timestamp").isNotNull(),
                datediff(
                    col("order_approved_at"),
>>>>>>> rescue-sdd
                    col("order_purchase_timestamp"),
                ),
            ).otherwise(None),
        )
        .withColumn(
<<<<<<< HEAD
            "is_delivered_on_time",
=======
            "is_late_delivery",
>>>>>>> rescue-sdd
            when(
                col("order_delivered_customer_date").isNotNull()
                & col("order_estimated_delivery_date").isNotNull(),
                col("order_delivered_customer_date")
<<<<<<< HEAD
                <= col("order_estimated_delivery_date"),
=======
                > col("order_estimated_delivery_date"),
>>>>>>> rescue-sdd
            ).otherwise(None),
        )
    )
