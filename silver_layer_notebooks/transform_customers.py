# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format('parquet').load("abfss://bronze@myprojecte2estorage.dfs.core.windows.net/customers")

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df.withColumn('domain_name',split(col('email'),'@').getItem(1))
df1.display()

# COMMAND ----------

df1.groupBy('domain_name').agg(count('customer_id').alias("cust_count")).orderBy(col('cust_count').desc()).display()

# COMMAND ----------

df1=df1.withColumn('full_name',concat(col('first_name'),lit(' '),col('last_name')))
df1 = df1.drop('first_name','last_name','_rescued_data')
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO databricks_catalog.silver.customers_silver
# MAGIC VALUES('C00001','rushjeff123@ryan.org','Denver','CO','ryan.org','Emily Mooney'),
# MAGIC ('C02001','shiva@ryan.org','Dallas','TX','ryan.org','Shiv Kumar')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_catalog.silver.customers_silver

# COMMAND ----------

df1.write.format('delta').mode('append').save("abfss://silver@myprojecte2estorage.dfs.core.windows.net/customers")
