import dlt
from pyspark.sql.functions import col, current_timestamp


@dlt.table(
    name="fct_orders",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "orders",
        "pipelines.autoOptimize.zOrderCols": "order_id",
    },
    comment="Gold layer — orders fact table joined with customer dimension",
)
def fct_orders():
    orders = dlt.read("silver_orders").select(
        col("order_id"),
        col("customer_id"),
        col("order_status"),
        col("order_purchase_timestamp"),
        col("order_approved_at"),
        col("order_delivered_carrier_date"),
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date"),
        col("delivery_delay_days"),
        col("order_processing_days"),
        col("is_late_delivery"),
    )

    customers = dlt.read("dim_customers").select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_city"),
        col("customer_state"),
        col("customer_location"),
    )

    return (
        orders.join(customers, on="customer_id", how="left")
        .withColumn("_gold_timestamp", current_timestamp())
    )
