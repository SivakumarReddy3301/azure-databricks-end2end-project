# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Loading

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://bronze@myprojecte2estorage.dfs.core.windows.net/products')
df = df.drop('_rescued_data')
display(df)

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products limit 100

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_catalog.bronze.discount_func(price DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN round(price * 0.90,2)

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, price, databricks_catalog.bronze.discount_func(price) as discounted_price from products

# COMMAND ----------

# MAGIC %md
# MAGIC #####using functions with df

# COMMAND ----------

df = df.withColumn('discounted_price',expr("databricks_catalog.bronze.discount_func(price)"))
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_catalog.bronze.upper_func(product_name STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS $$
# MAGIC return product_name.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, brand, databricks_catalog.bronze.upper_func(brand) as brand_upper from products

# COMMAND ----------

df.write.mode('append').format('delta').save('abfss://silver@myprojecte2estorage.dfs.core.windows.net/products')
