import dlt
from pyspark.sql.functions import col, current_timestamp


@dlt.table(
    name="dim_products",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "products",
        "pipelines.autoOptimize.zOrderCols": "product_id",
        "delta.enableChangeDataFeed": "false",
    },
    comment="Camada Gold - dimensao de produtos com categoria em ingles e volume",
)
def dim_products():
    return (
        dlt.read_stream("silver_products")
        .select(
            col("product_id"),
            col("product_category_name"),
            col("product_category_name_english"),
            col("product_weight_g"),
            col("product_length_cm"),
            col("product_height_cm"),
            col("product_width_cm"),
            col("product_volume_cm3"),
            current_timestamp().alias("_dimension_refresh_timestamp"),
        )
    )
