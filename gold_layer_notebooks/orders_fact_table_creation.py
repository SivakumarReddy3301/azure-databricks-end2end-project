# Databricks notebook source
# MAGIC %md
# MAGIC ##### Data loading

# COMMAND ----------

df = spark.sql("SELECT * FROM databricks_catalog.silver.orders_silver")
df.display()

# COMMAND ----------

df_dimcust = spark.sql("select dimCustomerKey,customer_id as dim_customer_id from databricks_catalog.gold.dimcustomers")
df_dimprod = spark.sql("select product_id as dimProductKey, product_id as dim_product_id from databricks_catalog.gold.dimproducts")

# COMMAND ----------

df_fact= df.join(df_dimcust, df.customer_id == df_dimcust.dim_customer_id, 'left').join(df_dimprod, df.product_id == df_dimprod.dim_product_id, 'left')
df_fact.display()
df_fact_new = df_fact.drop('dim_customer_id','dim_product_id','customer_id','product_id')



# COMMAND ----------
# SCD type 1
if spark.catalog.tableExists('databricks_catalog.gold.factorders):
    dlt_obj = DeltaTable.forName(spark, 'databricks_catalog.gold.factorders')
    dlt_obj.alias('target').merge(
        df_fact_new.alias('source'),
        'target.order_id = source.order_id AND target.dimCustomerKey = source.dimCustomerKey AND target.dimProductKey = source.dimProductKey')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
else:
    df_fact_new.write.mode('append')\
    option('path','abfss://gold@myprojecte2estorage.dfs.core.windows.net/factorders')\
    .saveAsTable('databricks_catalog.gold.factorders')

# COMMAND ----------

