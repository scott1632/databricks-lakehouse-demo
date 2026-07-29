# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold Aggregate
# MAGIC Builds the business-facing marts from the silver table: daily revenue
# MAGIC by category, and per-customer lifetime value.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
dbutils.widgets.text("schema", "lakehouse_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

try:
    from lakehouse.gold import customer_lifetime_value, daily_category_revenue
except ModuleNotFoundError:
    import os
    import sys

    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = os.path.dirname(os.path.dirname(notebook_path))
    sys.path.insert(0, f"/Workspace{repo_root}/src")

    from lakehouse.gold import customer_lifetime_value, daily_category_revenue

# COMMAND ----------

silver_table_name = f"{catalog}.{schema}.silver_orders"
silver_df = spark.table(silver_table_name)

revenue_df = daily_category_revenue(silver_df)
clv_df = customer_lifetime_value(silver_df)

display(revenue_df)
display(clv_df.limit(20))

# COMMAND ----------

revenue_table_name = f"{catalog}.{schema}.gold_daily_category_revenue"
clv_table_name = f"{catalog}.{schema}.gold_customer_ltv"

revenue_df.write.format("delta").mode("overwrite").saveAsTable(revenue_table_name)
clv_df.write.format("delta").mode("overwrite").saveAsTable(clv_table_name)

print("Gold marts ready:")
print(f"  {revenue_table_name}")
print(f"  {clv_table_name}")

# COMMAND ----------

from delta.tables import DeltaTable

DeltaTable.forName(spark, revenue_table_name).optimize().executeCompaction()
