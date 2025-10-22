# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Reading Data from bronze layer

df =spark.read.format("parquet")\
    .load("abfss://bronze@deltalakerg1.dfs.core.windows.net/products")

# Cleaning and Transform data for gold layer
df = df.drop("_rescued_data")\
       .withColumn("last_updated",current_timestamp())\
       .withColumn("brand",upper(col("brand")))
 

# FUNCTIONS

df.createOrReplaceTempView("products")

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_cata.bronze.discount_func(p_price double)
# MAGIC RETURNS double
# MAGIC LANGUAGE SQL
# MAGIC RETURN round(p_price * 0.90,2)

# COMMAND ----------

df = df.withColumn("discounted_price", expr("databricks_cata.bronze.discount_func(price)"))

# COMMAND ----------
# Write Data

df.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@deltalakerg1.dfs.core.windows.net/products")\
    .save()

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@deltalakerg1.dfs.core.windows.net/products'