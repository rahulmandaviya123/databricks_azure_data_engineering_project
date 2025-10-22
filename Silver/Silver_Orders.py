# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------
# Reading Data

df = spark.read.format("parquet")\
      .load("abfss://bronze@deltalakerg1.dfs.core.windows.net/orders")

# COMMAND ----------

df = df.withColumn("order_date",to_timestamp(col('order_date')))\
       .withColumn("year",year(col("order_date")))\
       .drop("_rescued_data")

 
#Assign Unique number using window function

df1 =df.withColumn("flag",dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

# Write Data to silver layer

df.write.format("delta")\
      .mode("overwrite")\
      .save("abfss://silver@deltalakerg1.dfs.core.windows.net/orders")

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@deltalakerg1.dfs.core.windows.net/orders'