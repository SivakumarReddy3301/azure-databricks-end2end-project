# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Data

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://bronze@myprojecte2estorage.dfs.core.windows.net/orders')

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df.drop('_rescued_data')

# COMMAND ----------

df1 =df1.withColumn('order_date',to_timestamp(df.order_date))

# COMMAND ----------

df1 = df1.withColumn('year',year(col('order_date')))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Window functions

# COMMAND ----------

window = Window.partitionBy(col('year')).orderBy(desc('total_amount'))
df2 = df1.withColumn('d_rank', dense_rank().over(window))
df2 = df2.withColumn('reg_rank', rank().over(window))
df2 = df2.withColumn('row_num', row_number().over(window))

# COMMAND ----------

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OOP Classes

# COMMAND ----------

df_new = df1

# COMMAND ----------

class windows:
  window = Window.partitionBy(col('year')).orderBy(desc('total_amount'))
  def dense_rank(self,df):
    df_dr = df.withColumn('d_rank', dense_rank().over(window))
    return df_dr
  def reg_rank(self,df):
    df_r = df.withColumn('reg_rank', rank().over(window))
    return df_r
  def row_num(self,df):
    df_rn = df.withColumn('row_num', row_number().over(window))
    return df_rn

# COMMAND ----------

windowObj = windows()
df_dr = windowObj.reg_rank(df_new)
display(df_dr)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

df.write.format('delta').mode('append').save('abfss://silver@myprojecte2estorage.dfs.core.windows.net/orders')
