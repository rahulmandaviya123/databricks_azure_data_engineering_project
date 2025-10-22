# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df =spark.read.format("parquet")\
    .load("abfss://bronze@deltalakerg1.dfs.core.windows.net/products")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.drop("_rescued_data")
df = df.withColumn("last_updated",current_timestamp())
 

# COMMAND ----------

df = df.withColumn("brand",upper(col("brand")))
 

# COMMAND ----------

# MAGIC %md
# MAGIC ## FUNCTIONS

# COMMAND ----------

df.createOrReplaceTempView("products")
 


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_cata.bronze.discount_func(p_price double)
# MAGIC RETURNS double
# MAGIC LANGUAGE SQL
# MAGIC RETURN round(p_price * 0.90,2)

# COMMAND ----------

df = df.withColumn("discounted_price", expr("databricks_cata.bronze.discount_func(price)"))

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@deltalakerg1.dfs.core.windows.net/products")\
    .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@deltalakerg1.dfs.core.windows.net/products'