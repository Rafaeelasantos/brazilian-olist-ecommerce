import dlt
from pyspark.sql.functions import current_timestamp


@dlt.table(
    name="silver_product_category",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "domain": "products",
        "pipelines.autoOptimize.zOrderCols": "product_category_name",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Silver layer — cleaned product category translation reference",
)
@dlt.expect_or_drop(
    "valid_category_name", "product_category_name IS NOT NULL"
)
def silver_product_category():
    return (
        dlt.read_stream("workspace.bronze.bronze_product_category")
        .select(
            "product_category_name",
            "product_category_name_english",
            "_ingest_timestamp",
            current_timestamp().alias("_processing_timestamp"),
        )
    )
