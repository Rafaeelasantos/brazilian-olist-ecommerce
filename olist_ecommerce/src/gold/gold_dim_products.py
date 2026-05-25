import dlt
from pyspark.sql.functions import current_timestamp


@dlt.table(
    name="dim_products",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "products",
        "pipelines.autoOptimize.zOrderCols": "product_id",
        "delta.enableChangeDataFeed": "false",
    },
    comment="Gold layer — product dimension enriched with English category and volume",
)
def dim_products():
    products = dlt.read("workspace.silver.silver_products")
    return (
        products.select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_volume_cm3",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "product_photos_qty",
        )
        .withColumn("_dimension_refresh_timestamp", current_timestamp())
    )
