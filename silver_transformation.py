# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

class transformations:

    def dedup(self, df: DataFrame, dedup_cols: List, cdc: str):
        df = df.withColumn(
            "dedupKey",
            concat(*dedup_cols)
        )
        df = df.withColumn(
            "dedupCounts",
            row_number().over(
                Window.partitionBy("dedupKey").orderBy(desc(cdc))
            )
        )
        df = df.filter(col('dedupCounts') == 1)
        df = df.drop("dedupKey", "dedupCounts")
        return df

    def process_timestamp(self, df):
        df = df.withColumn("process_timestamp", current_timestamp())
        return df

    def upsert(self, df, key_cols, table, cdc):
        merge_condition = " AND ".join(
            [f"src.{i}=trg.{i}" for i in key_cols]
        )
        dlt_obj = DeltaTable.forName(spark,f"pysparkdbt.silver.{table}")
        dlt_obj.alias("trg").merge(
            df.alias("src"),
            merge_condition
        ).whenMatchedUpdateAll(
            condition=f"src.{cdc} >= trg.{cdc}"
        ).whenNotMatchedInsertAll().execute()
        return 1

# COMMAND ----------

import os
import sys

# COMMAND ----------

current_dir = os.getcwd()
sys.path.append(current_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **CUSTOMERS**

# COMMAND ----------

df_cust = spark.read.table("pysparkdbt.bronze.customers")

# COMMAND ----------

display(df_cust)

# COMMAND ----------

df_cust = df_cust.withColumn("domain",split(col("email"), "@")[1])
display(df_cust)


# COMMAND ----------

df_cust = df_cust.withColumn("phone_number",regexp_replace("phone_number",r"[^0-9]",""))
display(df_cust)

# COMMAND ----------

df_cust = df_cust.withColumn("full name",concat_ws(" ", col("first_name"), col("last_name")))
df_cust = df_cust.drop("first_name", "last_name")
display(df_cust)
                             

# COMMAND ----------

# from utils.custom_utils import transformations


# COMMAND ----------

cust_obj = transformations()
cust_df_trns=cust_obj.dedup(df_cust,['customer_id'],'last_updated_timestamp')
display(cust_df_trns)

# COMMAND ----------

df_cust = cust_obj.process_timestamp(cust_df_trns)
display(df_cust)

# COMMAND ----------

# Rename columns with invalid characters
df_cust = df_cust.withColumnRenamed("full name", "full_name")

if not spark.catalog.tableExists("pysparkdbt.silver.customers"):
    df_cust.write.format("delta") \
        .mode("append") \
        .saveAsTable("pysparkdbt.silver.customers")
else:
    cust_obj.upsert(
        df_cust,
        ['customer_id'],
        'customers',
        'last_updated_timestamp'
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.silver.customers

# COMMAND ----------

# MAGIC %md
# MAGIC #### **DRIVERS**

# COMMAND ----------

df_driver = spark.read.table("pysparkdbt.bronze.drivers")
display(df_driver)

# COMMAND ----------

df_driver = df_driver.withColumn("phone_number",regexp_replace("phone_number",r"[^0-9]",""))

df_driver = df_driver.withColumn("full_name",concat_ws(" ", col("first_name"), col("last_name")))
df_driver = df_driver.drop("first_name", "last_name")
display(df_driver)

# COMMAND ----------

driver_obj = transformations()


# COMMAND ----------

df_driver = driver_obj.dedup(df_driver,['driver_id'],'last_updated_timestamp')

# COMMAND ----------

df_driver = driver_obj.process_timestamp(df_driver)


# COMMAND ----------

if not spark.catalog.tableExists("pysparkdbt.silver.drivers"):
    df_driver.write.format("delta") \
        .mode("append") \
        .saveAsTable("pysparkdbt.silver.drivers")
else:
    driver_obj.upsert(
        df_driver,
        ['driver_id'],
        'drivers',
        'last_updated_timestamp'
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.silver.drivers

# COMMAND ----------

# MAGIC %md
# MAGIC #### **LOCATIONS**

# COMMAND ----------

df_loc = spark.read.table("pysparkdbt.bronze.locations")
display(df_loc)


# COMMAND ----------

loc_obj = transformations()

# COMMAND ----------

df_loc = loc_obj.process_timestamp(df_loc)
df_loc = loc_obj.dedup(df_loc,['location_id'],'last_updated_timestamp')

# COMMAND ----------

if not spark.catalog.tableExists("pysparkdbt.silver.locations"):
    df_loc.write.format("delta") \
        .mode("append") \
        .saveAsTable("pysparkdbt.silver.locations")
else:
    loc_obj.upsert(
        df_loc,
        ['location_id'],
        'locations',
        'last_updated_timestamp'
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.silver.locations

# COMMAND ----------

# MAGIC %md
# MAGIC #### **PAYMENTS**

# COMMAND ----------

df_pay = spark.read.table("pysparkdbt.bronze.payments")
display(df_pay)



# COMMAND ----------

from pyspark.sql.functions import when, col

df_pay = df_pay.withColumn(
    "online_payment_status",
    when((col("payment_method") == 'Card') & (col("payment_status") == 'Success'), "Online-Success")
    .when((col("payment_method") == 'Card') & (col("payment_status") == 'Failed'), "Online-Failed")
    .when((col("payment_method") == 'Card') & (col("payment_status") == 'Pending'), "Online-Pending")
    .otherwise("Offline")
)

display(df_pay)

# COMMAND ----------

payment_obj = transformations()
df_pay = payment_obj.process_timestamp(df_pay)
df_pay = payment_obj.dedup(df_pay,['payment_id'],'last_updated_timestamp')
if not spark.catalog.tableExists("pysparkdbt.silver.payments"):
    df_pay.write.format("delta") \
        .mode("append") \
        .saveAsTable("pysparkdbt.silver.payments")    
else:
    payment_obj.upsert(           
        df_pay,
        ['payment_id'],
        'payments'        
        ,'last_updated_timestamp'
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.silver.payments

# COMMAND ----------

# MAGIC %md
# MAGIC #### **VEHICLES**

# COMMAND ----------

df_veh = spark.read.table("pysparkdbt.bronze.vehicles")
display(df_veh)

# COMMAND ----------

df_veh = df_veh.withColumn("make",upper(col("make")))
display(df_veh)

# COMMAND ----------

vehicle_obj = transformations()
df_veh = vehicle_obj.process_timestamp(df_veh)
df_veh = vehicle_obj.dedup(df_veh,['vehicle_id'],'last_updated_timestamp')


# COMMAND ----------

if not spark.catalog.tableExists("pysparkdbt.silver.vehicles"):
    df_veh.write.format("delta") \
        .mode("append") \
        .saveAsTable("pysparkdbt.silver.vehicles")
else:
    vehicle_obj.upsert(
        df_veh,
        ['vehicle_id'],
        'vehicles',
        'last_updated_timestamp'
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.silver.vehicles

# COMMAND ----------

