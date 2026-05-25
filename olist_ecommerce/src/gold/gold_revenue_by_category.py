import dlt
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    avg,
    round as spark_round,
)


@dlt.table(
    name="gold_revenue_by_category",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "products",
        "pipelines.autoOptimize.zOrderCols": "product_category_name_english",
    },
    comment="Gold layer — revenue and order metrics aggregated by product category",
)
def gold_revenue_by_category():
    items = dlt.read("silver_order_items").select(
        col("order_id"),
        col("product_id"),
        col("price"),
        col("freight_value"),
        col("total_item_value"),
    )

    products = dlt.read("silver_products").select(
        col("product_id"),
        col("product_category_name"),
        col("product_category_name_english"),
    )

    orders = dlt.read("silver_orders").select(
        col("order_id"),
        col("order_status"),
    )

    items_with_category = items.join(products, on="product_id", how="left")
    items_with_status = items_with_category.join(orders, on="order_id", how="left")

    delivered = items_with_status.filter(col("order_status") == "delivered")

    return (
        delivered.groupBy("product_category_name_english", "product_category_name")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            count("product_id").alias("total_items_sold"),
            spark_round(spark_sum("price"), 2).alias("total_revenue"),
            spark_round(spark_sum("freight_value"), 2).alias("total_freight"),
            spark_round(spark_sum("total_item_value"), 2).alias("total_gmv"),
            spark_round(avg("price"), 2).alias("avg_item_price"),
        )
        .orderBy(col("total_revenue").desc())
    )
