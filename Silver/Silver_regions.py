# Databricks notebook source
df = spark.read.table("databricks_cata.bronze.regions")

df = df.drop("_rescued_data")

df.write.format("delta")\
     .mode("overwrite")\
     .save("abfss://silver@deltalakerg1.dfs.core.windows.net/regions")

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@deltalakerg1.dfs.core.windows.net/regions'