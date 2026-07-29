"""Silver layer: deduplicated, quality-checked order records.

Reads the bronze append log and upserts (MERGE) the latest state per
order_id into the silver Delta table, keyed on the most recent event_time.
This is where basic data-quality rules are enforced (positive quantity,
known status, non-null keys).
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VALID_STATUSES = {"placed", "confirmed", "shipped", "delivered", "cancelled"}


def clean_bronze(df: DataFrame) -> DataFrame:
    """Apply data-quality filters and keep only the latest event per order."""
    quality_filtered = df.filter(
        (F.col("order_id").isNotNull())
        & (F.col("quantity") > 0)
        & (F.col("unit_price") > 0)
        & (F.col("status").isin(*VALID_STATUSES))
    )

    window = Window.partitionBy("order_id").orderBy(F.col("event_time").desc())
    return (
        quality_filtered.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("total_amount", F.round(F.col("unit_price") * F.col("quantity"), 2))
    )


def upsert_silver(spark: SparkSession, source_df: DataFrame, table_name: str) -> None:
    """Merge the cleaned bronze batch into the silver managed table on order_id."""
    if not spark.catalog.tableExists(table_name):
        source_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return

    target = DeltaTable.forName(spark, table_name)
    (
        target.alias("t")
        .merge(source_df.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll(condition="s.event_time > t.event_time")
        .whenNotMatchedInsertAll()
        .execute()
    )
