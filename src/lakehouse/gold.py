"""Gold layer: business-facing aggregates built from the silver table.

Two marts are produced:
  - daily_category_revenue: revenue and order volume by day and category
  - customer_lifetime_value: rolling per-customer spend and order count
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DELIVERED_STATUSES = ("delivered", "shipped", "confirmed")


def daily_category_revenue(silver_df: DataFrame) -> DataFrame:
    revenue_eligible = silver_df.filter(F.col("status").isin(*DELIVERED_STATUSES))
    return (
        revenue_eligible.withColumn("order_date", F.to_date("event_time"))
        .groupBy("order_date", "category")
        .agg(
            F.sum("total_amount").alias("revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("total_amount").alias("avg_order_value"),
        )
        .orderBy("order_date", "category")
    )


def customer_lifetime_value(silver_df: DataFrame) -> DataFrame:
    revenue_eligible = silver_df.filter(F.col("status").isin(*DELIVERED_STATUSES))
    return (
        revenue_eligible.groupBy("customer_id")
        .agg(
            F.sum("total_amount").alias("lifetime_value"),
            F.count("order_id").alias("total_orders"),
            F.max("event_time").alias("last_order_at"),
        )
        .orderBy(F.col("lifetime_value").desc())
    )
