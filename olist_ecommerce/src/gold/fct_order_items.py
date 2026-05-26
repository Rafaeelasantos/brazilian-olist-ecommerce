import dlt
from pyspark.sql.functions import col, current_timestamp


@dlt.table(
    name="fct_order_items",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "order_items",
        "pipelines.autoOptimize.zOrderCols": "order_id",
    },
    comment="Gold layer — order items fact table joined with order and product dimensions",
)
def fct_order_items():
    items = dlt.read("silver_order_items").select(
        col("order_id"),
        col("order_item_id"),
        col("product_id"),
        col("seller_id"),
        col("shipping_limit_date"),
        col("price"),
        col("freight_value"),
        col("total_item_value"),
    )

    orders = dlt.read("fct_orders").select(
        col("order_id"),
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_city"),
        col("customer_state"),
        col("order_status"),
        col("order_purchase_timestamp"),
        col("order_delivered_customer_date"),
        col("is_late_delivery"),
    )

    products = dlt.read("dim_products").select(
        col("product_id"),
        col("product_category_name_english"),
        col("product_weight_g"),
        col("product_volume_cm3"),
    )

    return (
        items.join(orders, on="order_id", how="left")
        .join(products, on="product_id", how="left")
        .withColumn("_gold_timestamp", current_timestamp())
    )
