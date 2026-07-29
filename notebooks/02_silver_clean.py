# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver Clean
# MAGIC Reads the bronze log, applies data-quality rules, deduplicates replayed
# MAGIC events down to the latest state per `order_id`, and MERGEs the result
# MAGIC into the silver Delta table.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
dbutils.widgets.text("schema", "lakehouse_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

try:
    from lakehouse.silver import clean_bronze, upsert_silver
except ModuleNotFoundError:
    import os
    import sys

    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = os.path.dirname(os.path.dirname(notebook_path))
    sys.path.insert(0, f"/Workspace{repo_root}/src")

    from lakehouse.silver import clean_bronze, upsert_silver

# COMMAND ----------

bronze_table_name = f"{catalog}.{schema}.bronze_orders"
silver_table_name = f"{catalog}.{schema}.silver_orders"

bronze_df = spark.table(bronze_table_name)
silver_df = clean_bronze(bronze_df)

print(f"Bronze rows: {bronze_df.count()}  ->  Silver rows after dedup/quality filter: {silver_df.count()}")
display(silver_df.limit(10))

# COMMAND ----------

upsert_silver(spark, silver_df, silver_table_name)

print(f"Silver table ready at {silver_table_name}")
