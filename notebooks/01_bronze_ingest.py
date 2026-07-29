# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze Ingest
# MAGIC Generates a batch of synthetic order events and appends them, as-is, to
# MAGIC the bronze Delta table. No cleaning happens here — bronze is the
# MAGIC immutable raw log that every downstream layer can be rebuilt from.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
dbutils.widgets.text("schema", "lakehouse_demo", "Schema")
dbutils.widgets.text("event_count", "2000", "Number of synthetic events to generate")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
event_count = int(dbutils.widgets.get("event_count"))

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC The `lakehouse` package is installed as a wheel library on this job's
# MAGIC cluster (see `databricks.yml`), so it imports like any other package —
# MAGIC no path hacks needed.

# COMMAND ----------

from lakehouse.bronze import events_to_bronze_df, write_bronze
from lakehouse.data_generator import generate_orders

# COMMAND ----------

events = generate_orders(n=event_count, seed=None)
bronze_df = events_to_bronze_df(spark, events)

print(f"Generated {bronze_df.count()} raw events (including simulated duplicate replays)")
display(bronze_df.limit(10))

# COMMAND ----------

bronze_table_path = f"/Volumes/{catalog}/{schema}/lakehouse/bronze_orders"
write_bronze(bronze_df, bronze_table_path)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_orders
    USING DELTA
    LOCATION '{bronze_table_path}'
    """
)

print(f"Bronze table ready at {bronze_table_path}")
