import dlt
from pyspark.sql.functions import col, current_timestamp


@dlt.table(
    name="dim_products",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "products",
        "pipelines.autoOptimize.zOrderCols": "product_id",
    },
    comment="Gold layer — product dimension enriched with English category name and volume",
)
def dim_products():
    return (
        dlt.read("silver_products")
        .select(
            col("product_id"),
            col("product_category_name"),
            col("product_category_name_english"),
            col("product_name_length"),
            col("product_description_length"),
            col("product_photos_qty"),
            col("product_weight_g"),
            col("product_length_cm"),
            col("product_height_cm"),
            col("product_width_cm"),
            col("product_volume_cm3"),
            current_timestamp().alias("_gold_timestamp"),
        )
    )
