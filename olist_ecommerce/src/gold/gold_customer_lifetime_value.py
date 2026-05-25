import dlt
from pyspark.sql.functions import col, count, sum as spark_sum, avg, round as spark_round, min as spark_min, max as spark_max, countDistinct


@dlt.table(
    name="gold_customer_lifetime_value",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "customers",
        "pipelines.autoOptimize.zOrderCols": "customer_unique_id",
    },
    comment="Gold layer — customer lifetime value: total spend, order count and avg ticket per unique customer",
)
def gold_customer_lifetime_value():
    # Filter current SCD Type 2 records (no end date = current version)
    customers = (
        dlt.read("silver_customers")
        .filter(col("__END_AT").isNull())
        .select(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_state"),
        )
    )

    orders = dlt.read("silver_orders").select(
        col("order_id"),
        col("customer_id"),
        col("order_status"),
        col("order_purchase_timestamp"),
    )

    items_agg = (
        dlt.read("silver_order_items")
        .groupBy("order_id")
        .agg(
            spark_round(spark_sum("total_item_value"), 2).alias("order_value"),
        )
    )

    return (
        orders.join(customers, on="customer_id", how="left")
        .join(items_agg, on="order_id", how="left")
        .groupBy("customer_unique_id", "customer_state")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_round(spark_sum("order_value"), 2).alias("total_spend"),
            spark_round(avg("order_value"), 2).alias("avg_order_value"),
            spark_min("order_purchase_timestamp").alias("first_order_date"),
            spark_max("order_purchase_timestamp").alias("last_order_date"),
        )
    )
