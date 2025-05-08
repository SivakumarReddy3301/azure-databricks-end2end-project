# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading from Source to Bronze

# COMMAND ----------

dbutils.widgets.text('file_name','orders')

# COMMAND ----------

p_file_name = dbutils.widgets.get('file_name')
print(p_file_name)

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
               .option("cloudFiles.format", "parquet")\
               .option("cloudFiles.schemaLocation", f"abfss://bronze@myprojecte2estorage.dfs.core.windows.net/checkpoint_{p_file_name}")\
                .load(f"abfss://source@myprojecte2estorage.dfs.core.windows.net/{p_file_name}")

# COMMAND ----------

df.writeStream.format("parquet").outputMode("append")\
              .option("checkpointLocation", f"abfss://bronze@myprojecte2estorage.dfs.core.windows.net/checkpoint_{p_file_name}")\
              .option("path",f"abfss://bronze@myprojecte2estorage.dfs.core.windows.net/{p_file_name}")\
              .trigger(once=True)\
              .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### CHECK THE DATA

# COMMAND ----------

df = spark.read.format('parquet').load(f"abfss://bronze@myprojecte2estorage.dfs.core.windows.net/{p_file_name}")
display(df)
