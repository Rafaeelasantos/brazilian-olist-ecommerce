import dlt
from pyspark.sql.functions import current_timestamp, col


@dlt.table(
    name="bronze_orders",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "domain": "orders",
        "pipelines.autoOptimize.zOrderCols": "order_id",
        "delta.enableChangeDataFeed": "true",
    },
    comment="Bronze layer — raw ingestion of orders via Auto Loader",
)
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("inferSchema", "false")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/workspace/default/ecommerce_raw_volume/_schemas/orders",
        )
        .option("cloudFiles.schemaEvolutionMode", "none")
        .load("/Volumes/workspace/default/ecommerce_raw_volume/olist_orders_dataset.csv/")
        .select(
            "*",
            current_timestamp().alias("_ingest_timestamp"),
            col("_metadata.file_path").alias("_source_file"),
        )
    )
