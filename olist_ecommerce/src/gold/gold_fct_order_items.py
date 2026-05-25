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
    comment="Gold layer — fact order items joined with product dimension",
)
def fct_order_items():
    items = dlt.read("workspace.silver.silver_order_items")
    products = dlt.read("workspace.gold.dim_products")

    return (
        items.join(products, on="product_id", how="inner")
        .select(
            items["order_id"],
            items["order_item_id"],
            items["product_id"],
            items["seller_id"],
            items["shipping_limit_date"],
            items["price"],
            items["freight_value"],
            items["total_item_value"],
            # Product dimension attributes
            products["product_category_name"],
            products["product_category_name_english"],
            products["product_volume_cm3"],
            products["product_weight_g"],
        )
        .withColumn("_fact_processing_timestamp", current_timestamp())
    )
