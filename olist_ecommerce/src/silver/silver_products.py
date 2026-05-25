import dlt
from pyspark.sql.functions import current_timestamp, col, when


@dlt.table(
    name="silver_products",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "products",
        "pipelines.autoOptimize.zOrderCols": "product_id",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Silver layer — cleaned products enriched with category name (English)",
)
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_weight", "product_weight_g > 0")
def silver_products():
    products = dlt.read_stream("workspace.bronze.bronze_products")
    categories = dlt.read("workspace.bronze.bronze_product_category")

    return (
        products.join(
            categories.select("product_category_name", "product_category_name_english"),
            on="product_category_name",
            how="left",
        )
        .select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "_ingest_timestamp",
            (
                col("product_length_cm")
                * col("product_height_cm")
                * col("product_width_cm")
            ).alias("product_volume_cm3"),
            when(col("product_category_name_english").isNotNull(), True)
            .otherwise(False)
            .alias("has_english_category"),
            current_timestamp().alias("_processing_timestamp"),
        )
    )
