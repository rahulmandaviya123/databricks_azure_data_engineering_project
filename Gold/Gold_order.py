# Databricks notebook source
# MAGIC %md
# MAGIC ## FACT ORDERS
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA READING

# COMMAND ----------

df = spark.sql("select * from databricks_cata.silver.orders_silver")

# COMMAND ----------

df_dimcus = spark.sql("select DimCustomerKey,customer_id as dim_customer_id from databricks_cata.gold.dimcustomers")
df_dimpor = spark.sql("select product_id as DimProductKey,product_id as dim_product_id from databricks_cata.gold.dimproducts")

# COMMAND ----------

df1 = spark.sql("select * from databricks_cata.gold.dimproducts")

# COMMAND ----------

df_fact = df.join(df_dimcus, df['customer_id']==df_dimcus['dim_customer_id'],how='left').join(df_dimpor, df['product_id']==df_dimpor['dim_product_id'],how='left')

df_fact_new = df_fact.drop('dim_customer_id','dim_product_id','customer_id','product_id')
 

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

if  spark.catalog.tableExists("databricks_cata.gold.FactOrdes"):
    dlt_obj = DeltaTable.forName(spark, "databricks_cata.gold.FactOrdes")
    dlt_obj.alias("trg").merge(df_fact_new.alias("src"),"trg.order_id = src.order_id AND trg.DimCustomerKey = src.DimCustomerKey AND trg.DimProductKey = src.DimProductKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_fact_new.write.format("delta")\
        .option("path", "abfss://gold@deltalakerg1.dfs.core.windows.net/FactOrdes")\
        .saveAsTable("databricks_cata.gold.FactOrdes")

  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.factordes

# COMMAND ----------

