# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

df = spark.read.format("parquet")\
       .load("abfss://bronze@deltalakerg1.dfs.core.windows.net/customers")

# COMMAND ----------
# Cleaning and Transform data for silver layer

df = df.withColumn("domain" ,split(col("email"),"@")[1])\
       .withColumn("full_name", concat(col("first_name"),lit(" "),col("last_name")))\
       .drop("_rescued_data")\
       .withColumn("last_updated", current_timestamp())\
       .drop("first_name","last_name")


df.write.format("delta")\
       .mode("overwrite")\
       .save("abfss://silver@deltalakerg1.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.customers_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@deltalakerg1.dfs.core.windows.net/customers'