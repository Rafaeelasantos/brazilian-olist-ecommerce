import dlt
from pyspark.sql.functions import col, count, avg, round as spark_round, sum as spark_sum, when


@dlt.table(
    name="gold_delivery_performance",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "orders",
        "pipelines.autoOptimize.zOrderCols": "customer_state",
    },
    comment="Gold layer — delivery performance metrics: on-time rate and avg delivery days by state and order status",
)
def gold_delivery_performance():
    # Filter current SCD Type 2 records (no end date = current version)
    customers = (
        dlt.read("silver_customers")
        .filter(col("__END_AT").isNull())
        .select(
            col("customer_id"),
            col("customer_state"),
        )
    )

    orders = dlt.read("silver_orders").select(
        col("order_id"),
        col("customer_id"),
        col("order_status"),
        col("delivery_days"),
        col("is_delivered_on_time"),
    )

    return (
        orders.join(customers, on="customer_id", how="left")
        .groupBy("customer_state", "order_status")
        .agg(
            count("order_id").alias("total_orders"),
            spark_round(avg("delivery_days"), 1).alias("avg_delivery_days"),
            spark_sum(when(col("is_delivered_on_time") == True, 1).otherwise(0)).alias("delivered_on_time"),
            spark_sum(when(col("is_delivered_on_time") == False, 1).otherwise(0)).alias("delivered_late"),
        )
        .withColumn(
            "on_time_rate",
            spark_round(
                col("delivered_on_time") / when(col("total_orders") > 0, col("total_orders")).otherwise(None),
                4,
            ),
        )
    )
