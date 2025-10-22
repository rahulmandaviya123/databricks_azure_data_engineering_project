# Databricks notebook source
df = spark.read.table("databricks_cata.bronze.regions")

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.write.format("delta")\
     .mode("overwrite")\
     .save("abfss://silver@deltalakerg1.dfs.core.windows.net/regions")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@deltalakerg1.dfs.core.windows.net/regions'