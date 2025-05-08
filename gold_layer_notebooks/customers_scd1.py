# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Loading

# COMMAND ----------

df_cust_silver = spark.sql("SELECT * FROM databricks_catalog.silver.customers_silver")

# COMMAND ----------

display(df_cust_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Remove Duplicates

# COMMAND ----------

df_cust_silver = df_cust_silver.dropDuplicates(subset=['customer_id'])
df_cust_silver.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create new columns and if the table does not exist(for the first time) create it. Else if it exists, proceed with SCD logic

# COMMAND ----------

    

table_exists = spark.catalog.tableExists("databricks_catalog.gold.DimCustomers")
window = Window.orderBy('customer_id')
max_key = 0
df_cust_silver = df_cust_silver\
    .withColumn('created_date', current_timestamp())\
    .withColumn('end_date', lit(None).cast("timestamp"))\
    .withColumn('is_current', lit(True))\
    .withColumn('rn', row_number().over(window))\
    .withColumn('dimCustomerKey', col('rn')+lit(max_key))\
    .drop('rn')
if not table_exists:
    # create the initial table
    df_cust_silver.write.mode('overwrite')\
        .saveAsTable('databricks_catalog.gold.DimCustomers')
    
else:
    # Load the existing dim_customers table
    target_table = DeltaTable.forName(spark, "databricks_catalog.gold.DimCustomers")
    df_existing = target_table.toDF()

    # get max key
    max_key = spark.sql("SELECT MAX(dimCustomerKey) as max_key FROM databricks_catalog.gold.DimCustomers").collect()[0]['max_key']

    df_existing = df_existing.withColumn('rn', row_number().over(window))\
                            .withColumn('dimCustomerKey', col('rn')+lit(max_key))\
                            .drop('rn')
    # Perform SCD Type 1
    target_table.alias("tgt").merge(
        df_cust_silver.alias("src"),
        "tgt.dimCustomerKey = src.dimCustomerKey"
    ).whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

    # Perform SCD Type 2
    # df_changes = df_existing.alias('target')\
    #                         .join(df_cust_silver.alias('source'), on='dimCustomerKey', how='inner')\
    #                         .where(
    #                             "target.is_current" = true AND ()
    #                         )


# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_catalog.gold.DimCustomers
