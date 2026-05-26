import dlt
from pyspark.sql.functions import current_timestamp


@dlt.table(
    name="fct_order_items",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "order_items",
        "pipelines.autoOptimize.zOrderCols": "order_id",
        "delta.enableChangeDataFeed": "false",
    },
    comment="Camada Gold - tabela fato unica do star schema (grain = item de pedido)",
)
def fct_order_items():
    items = dlt.read_stream("silver_order_items")
    orders = dlt.read_stream("silver_orders")
    dim_cust = dlt.read_stream("dim_customers")
    dim_prod = dlt.read_stream("dim_products")

    return (
        items.join(orders, on="order_id", how="inner")
        .join(dim_cust, on="customer_id", how="inner")
        .join(dim_prod, on="product_id", how="inner")
        .select(
            items["order_id"],
            items["order_item_id"],
            items["product_id"],
            orders["customer_id"],
            items["seller_id"],
            orders["order_status"],
            orders["order_purchase_timestamp"],
            orders["order_approved_at"],
            orders["order_delivered_customer_date"],
            orders["order_estimated_delivery_date"],
            items["shipping_limit_date"],
            orders["delivery_delay_days"],
            orders["order_processing_days"],
            orders["is_late_delivery"],
            items["price"],
            items["freight_value"],
            items["total_item_value"],
            dim_cust["customer_city"],
            dim_cust["customer_state"],
            dim_cust["customer_location"],
            dim_prod["product_category_name"],
            dim_prod["product_category_name_english"],
            dim_prod["product_volume_cm3"],
        )
        .withColumn("_fact_processing_timestamp", current_timestamp())
    )
