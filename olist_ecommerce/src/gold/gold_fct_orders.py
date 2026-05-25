import dlt
from pyspark.sql.functions import current_timestamp


@dlt.table(
    name="fct_orders",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "orders",
        "pipelines.autoOptimize.zOrderCols": "order_id",
        "delta.enableChangeDataFeed": "false",
    },
    comment="Gold layer — fact orders joined with current customer dimension",
)
def fct_orders():
    orders = dlt.read("workspace.silver.silver_orders")
    customers = dlt.read("workspace.gold.dim_customers")

    return (
        orders.join(customers, on="customer_id", how="inner")
        .select(
            orders["order_id"],
            orders["customer_id"],
            orders["order_status"],
            orders["order_purchase_timestamp"],
            orders["order_approved_at"],
            orders["order_delivered_carrier_date"],
            orders["order_delivered_customer_date"],
            orders["order_estimated_delivery_date"],
            # Derived
            orders["delivery_delay_days"],
            orders["order_processing_days"],
            orders["is_late_delivery"],
            # Customer dimension attributes
            customers["customer_unique_id"],
            customers["customer_city"],
            customers["customer_state"],
            customers["customer_location"],
        )
        .withColumn("_fact_processing_timestamp", current_timestamp())
    )
