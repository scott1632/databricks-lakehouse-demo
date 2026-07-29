"""Bronze layer: append-only raw ingestion into Delta Lake.

The bronze table is an immutable log of every event as received, including
duplicates and late-arriving replays. No cleaning or dedup happens here by
design, so downstream layers can always be reprocessed from source.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

BRONZE_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("sku", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("status", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("ingested_at", StringType(), False),
    ]
)


def events_to_bronze_df(spark: SparkSession, events: list[dict]) -> DataFrame:
    """Build the bronze DataFrame from raw event dicts, typing timestamps."""
    df = spark.createDataFrame(events, schema=BRONZE_SCHEMA)
    return df.withColumn("event_time", F.to_timestamp("event_time")).withColumn(
        "ingested_at", F.to_timestamp("ingested_at")
    )


def write_bronze(df: DataFrame, table_name: str) -> None:
    """Append new events to the bronze managed Delta table."""
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )
