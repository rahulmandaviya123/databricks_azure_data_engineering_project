# Databricks notebook source
# MAGIC %md
# MAGIC # **DLT Pipeline** 

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

my_rules = {
    "rule1": "product_id is not null",
    "rule2": "product_name is not null"
}

@dlt.table(
    name="Dimproducts_stg1"
)
@dlt.expect_all_or_drop(my_rules)
def Dimproducts_stg1():
    df = spark.read.table("databricks_cata.silver.products_silver")
    return df

# COMMAND ----------

#create destination silver table
dlt.create_streaming_table(
 name = "product_enr"
)
@dlt.view 
def products_enr_trns1():
    df = spark.readStream.table("Dimproducts_stg1") 
    return df

dlt.create_auto_cdc_flow(
    target = "product_enr",
    source = "products_enr_trns1",
    keys =["product_id"],
    sequence_by= "last_updated",
    stored_as_scd_type = 1
)

# COMMAND ----------

dlt.create_streaming_table(
    name = "dimproducts"
)

# AUTO CDC FLOW

dlt.create_auto_cdc_flow(
    target = "dimproducts",
    source = "products_enr_trns1",
    keys =["product_id"],
    sequence_by= "last_updated",
    stored_as_scd_type=2
)

# COMMAND ----------

