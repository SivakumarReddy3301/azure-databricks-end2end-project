# Databricks notebook source
df = spark.read.table('databricks_catalog.bronze.regions')
df.drop('_rescued_data')
display(df)

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('abfss://silver@myprojecte2estorage.dfs.core.windows.net/regions')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### CHECK THE DATA

# COMMAND ----------

df_regions = spark.read.format('delta').load('abfss://silver@myprojecte2estorage.dfs.core.windows.net/regions')
display(df_regions)

# COMMAND ----------

df_customers = spark.read.format('delta').load('abfss://silver@myprojecte2estorage.dfs.core.windows.net/customers')
display(df_customers)

# COMMAND ----------

df_orders = spark.read.format('delta').load('abfss://silver@myprojecte2estorage.dfs.core.windows.net/orders')
display(df_orders)

# COMMAND ----------

df_products = spark.read.format('delta').load('abfss://silver@myprojecte2estorage.dfs.core.windows.net/products')
display(df_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.customers_silver
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@myprojecte2estorage.dfs.core.windows.net/customers"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@myprojecte2estorage.dfs.core.windows.net/orders"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@myprojecte2estorage.dfs.core.windows.net/products"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@myprojecte2estorage.dfs.core.windows.net/regions"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED databricks_catalog.silver.products_silver;
# MAGIC
