"""Bronze layer: append-only raw ingestion into Delta Lake.

The bronze table is an immutable log of every event as received, including
duplicates and late-arriving replays. No cleaning or dedup happens here by
design, so downstream layers can always be reprocessed from source.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

BRONZE_SCHEMA = StructType(
    [
        # order_id/customer_id are nullable: bronze is the raw, unclean log
        # (per the module docstring), and silver's clean_bronze is what
        # actually filters out nulls in these fields — a non-nullable bronze
        # schema would reject bad rows before silver ever got a chance to.
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
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
    """Build the bronze DataFrame from raw event dicts, typing timestamps.

    unit_price is cast to a fixed-point decimal here rather than left as a
    double: summing many rounded doubles (e.g. in gold's lifetime-value
    aggregate) accumulates binary floating-point drift, which decimal
    arithmetic doesn't have.
    """
    df = spark.createDataFrame(events, schema=BRONZE_SCHEMA)
    return (
        df.withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .withColumn("unit_price", F.col("unit_price").cast(DecimalType(10, 2)))
    )


def write_bronze(df: DataFrame, table_name: str) -> None:
    """Append new events to the bronze managed Delta table."""
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )
