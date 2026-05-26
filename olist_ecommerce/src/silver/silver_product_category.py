import dlt
<<<<<<< HEAD
from pyspark.sql.functions import col, lower, trim
=======
from pyspark.sql.functions import col, lower, trim, current_timestamp
>>>>>>> rescue-sdd


@dlt.table(
    name="silver_product_category",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "products",
        "pipelines.autoOptimize.zOrderCols": "product_category_name",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Silver layer — cleansed product category name translations",
)
<<<<<<< HEAD
@dlt.expect_or_drop("category_name is not null", "product_category_name IS NOT NULL")
=======
@dlt.expect_or_drop("valid_category_name", "product_category_name IS NOT NULL")
>>>>>>> rescue-sdd
def silver_product_category():
    return (
        dlt.read_stream("bronze_product_category")
        .select(
            trim(lower(col("product_category_name"))).alias("product_category_name"),
<<<<<<< HEAD
            trim(lower(col("product_category_name_english"))).alias("product_category_name_english"),
            col("_ingest_timestamp"),
            col("_source_file"),
=======
            trim(lower(col("product_category_name_english"))).alias(
                "product_category_name_english"
            ),
            col("_ingest_timestamp"),
            col("_source_file"),
            current_timestamp().alias("_processing_timestamp"),
>>>>>>> rescue-sdd
        )
    )
