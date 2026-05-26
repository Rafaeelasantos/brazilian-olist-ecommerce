import dlt
from pyspark.sql.functions import col, trim, lower, current_timestamp


@dlt.table(
    name="silver_products",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "products",
        "pipelines.autoOptimize.zOrderCols": "product_id",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Silver layer — cleaned products enriched with English category name and volume",
)
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def silver_products():
    products = (
        dlt.read_stream("bronze_products")
        .select(
            col("product_id").cast("string"),
            trim(lower(col("product_category_name"))).alias("product_category_name"),
            col("product_name_lenght").cast("integer").alias("product_name_length"),
            col("product_description_lenght").cast("integer").alias("product_description_length"),
            col("product_photos_qty").cast("integer"),
            col("product_weight_g").cast("double"),
            col("product_length_cm").cast("double"),
            col("product_height_cm").cast("double"),
            col("product_width_cm").cast("double"),
            col("_ingest_timestamp"),
            col("_source_file"),
            current_timestamp().alias("_processing_timestamp"),
        )
        .withColumn(
            "product_volume_cm3",
            col("product_length_cm") * col("product_height_cm") * col("product_width_cm"),
        )
    )

    category = dlt.read("silver_product_category").select(
        col("product_category_name"),
        col("product_category_name_english"),
    )

    return products.join(
        category,
        on="product_category_name",
        how="left",
    )
