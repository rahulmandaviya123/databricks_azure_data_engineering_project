# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("init_load_flag", "0")
init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

df =spark.read.table("databricks_cata.silver.customers_silver")

# COMMAND ----------

df = df.dropDuplicates(subset = ['customer_id'])
df = df.drop("last_updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering new vs old record

# COMMAND ----------

if init_load_flag == 0:

    df_old = spark.sql('''select DimCustomerKey, customer_id,create_date,update_date 
                            from databricks_cata.gold.Dimcustomers''')
    
else:

    df_old = spark.sql('''select 0 DimCustomerKey, 0 customer_id,0 create_date,0 update_date 
                            from databricks_cata.silver.customers_silver where 1=0''')
 

# COMMAND ----------

display(df_old)

# COMMAND ----------

df_old =df_old.withColumnRenamed("DimCustomerKey", "DimCustomerKey_old")\
                                .withColumnRenamed("customer_id", "customer_id_old")\
                                .withColumnRenamed("create_date", "create_date_old")\
                                .withColumnRenamed("update_date", "update_date_old")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying join

# COMMAND ----------

df_join = df.join(df_old, df.customer_id == df_old.customer_id_old, "left")
 



# COMMAND ----------

display(df_join)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Seperating old vs new records

# COMMAND ----------

df_new = df_join.filter(df_join.DimCustomerKey_old.isNull())
 

# COMMAND ----------

df_old = df_join.filter(df_join.DimCustomerKey_old.isNotNull())
 

# COMMAND ----------

# Dropping all the coiumn which are not required

df_old = df_old.drop("customer_id_old","update_date_old")

# Renaming old_create_date column to create_date

df_old = df_old.withColumnRenamed("DimCustomerKey_old", "DimCustomerKey")
df_old = df_old.withColumnRenamed("create_date_old", "create_date")
df_old = df_old.withColumn("create_date", to_timestamp(col("create_date")))
#Recreating update_date column with current timestamp

df_old = df_old.withColumn("update_date", current_timestamp())

# COMMAND ----------

# Dropping all the coiumn which are not required

df_new = df_new.drop("DimCustomerKey_old","customer_id_old","update_date_old","create_date_old")

# Recreating Update date and create date columns with current timstamps

 
df_new = df_new.withColumn("create_date", current_timestamp())
df_new = df_new.withColumn("update_date", current_timestamp())
 
 

 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Surrogate Key - All Values

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", monotonically_increasing_id()+lit(1))

# COMMAND ----------

if init_load_flag ==1:
    max_surrogate_key = 0
else:
    df_maxsur = spark.sql("select max(DimCustomerKey) as max_surrogate_key from databricks_cata.gold.Dimcustomers")
    max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", lit(max_surrogate_key)+col("DimCustomerKey"))

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_cata.gold.Dimcustomers"):
    deltaTable = DeltaTable.forPath(
        spark,
        "abfss://gold@deltalakerg1.dfs.core.windows.net/Dimcustomers"
    )
    (
        deltaTable.alias("trg")
        .merge(
            df_final.alias("src"),
            "trg.customer_id = src.customer_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        df_final.write.format("delta")
        .option("path", "abfss://gold@deltalakerg1.dfs.core.windows.net/Dimcustomers")
        .mode("overwrite")
        .saveAsTable("databricks_cata.gold.Dimcustomers")
    )

# COMMAND ----------

df = spark.sql("select * from databricks_cata.gold.product_enr")
display(df)