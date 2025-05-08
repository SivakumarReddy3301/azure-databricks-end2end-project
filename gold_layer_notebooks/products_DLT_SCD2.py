# Databricks notebook source
# MAGIC %md
# MAGIC ##### Streaming Table

# COMMAND ----------



# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# expectation
my_rules={
    "rule1":"product_id is not null",
    "rule2":"product_name is not null"
}

# COMMAND ----------

@dlt.table()
@dlt.expect_all_or_drop(my_rules)
def DimProductsStage():
  df= spark.readStream.table("databricks_catalog.silver.products_silver")
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Streaming View

# COMMAND ----------

@dlt.view
def DimProductsView():
  df = spark.readStream.table("Live.DimProductsStage")
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### DimProducts dimension

# COMMAND ----------

dlt.create_streaming_table("DimProducts")

dlt.apply_changes(
  target = "DimProducts",
  source = "Live.DimProductsView",
  keys = ["product_id"],
  sequence_by = col("product_id"),
  stored_as_scd_type = 2
)
