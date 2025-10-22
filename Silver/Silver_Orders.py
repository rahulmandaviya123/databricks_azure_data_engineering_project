# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format("parquet")\
      .load("abfss://bronze@deltalakerg1.dfs.core.windows.net/orders")

# COMMAND ----------

df = df.withColumn("order_date",to_timestamp(col('order_date')))

# COMMAND ----------

df = df.withColumn("year",year(col("order_date")))
df = df.drop("_rescued_data")

# COMMAND ----------

display(df)

# COMMAND ----------

df1 =df.withColumn("flag",dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@deltalakerg1.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@deltalakerg1.dfs.core.windows.net/orders'