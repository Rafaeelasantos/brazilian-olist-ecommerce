import dlt
from pyspark.sql.functions import current_timestamp, col


@dlt.table(
    name="bronze_order_items",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "domain": "order_items",
        "pipelines.autoOptimize.zOrderCols": "order_id",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Bronze layer — raw ingestion of order items via Auto Loader",
)
def bronze_order_items():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("inferSchema", "false")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/workspace/default/ecommerce_raw_volume/_schemas/order_items",
        )
        .option("cloudFiles.schemaEvolutionMode", "none")
        .load("/Volumes/workspace/default/ecommerce_raw_volume/olist_order_items_dataset.csv/")
        .select(
            "*",
            current_timestamp().alias("_ingest_timestamp"),
            col("_metadata.file_path").alias("_source_file"),
        )
    )
