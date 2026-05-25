import dlt
from pyspark.sql.functions import current_timestamp, col


@dlt.table(
    name="bronze_product_category",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "domain": "product_category",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Bronze layer — raw ingestion of product category name translations via Auto Loader",
)
def bronze_product_category():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("inferSchema", "false")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/workspace/default/ecommerce_raw_volume/_schemas/product_category",
        )
        .option("cloudFiles.schemaEvolutionMode", "none")
        .load("/Volumes/workspace/default/ecommerce_raw_volume/product_category/")
        .select(
            "*",
            current_timestamp().alias("_ingest_timestamp"),
            col("_metadata.file_path").alias("_source_file"),
        )
    )
