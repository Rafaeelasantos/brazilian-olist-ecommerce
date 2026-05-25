import dlt
from pyspark.sql.functions import col, count, sum as spark_sum, avg, round as spark_round


@dlt.table(
    name="gold_orders_summary",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "orders",
        "pipelines.autoOptimize.zOrderCols": "order_id",
    },
    comment="Gold layer — order-level summary joining orders with aggregated items",
)
def gold_orders_summary():
    orders = dlt.read("silver_orders").select(
        col("order_id"),
        col("customer_id"),
        col("order_status"),
        col("order_purchase_timestamp"),
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date"),
        col("delivery_days"),
        col("is_delivered_on_time"),
    )

    items_agg = (
        dlt.read("silver_order_items")
        .groupBy("order_id")
        .agg(
            count("order_item_id").alias("total_items"),
            spark_round(spark_sum("price"), 2).alias("total_price"),
            spark_round(spark_sum("freight_value"), 2).alias("total_freight"),
            spark_round(spark_sum("total_item_value"), 2).alias("total_order_value"),
            spark_round(avg("price"), 2).alias("avg_item_price"),
        )
    )

    return orders.join(items_agg, on="order_id", how="left")
