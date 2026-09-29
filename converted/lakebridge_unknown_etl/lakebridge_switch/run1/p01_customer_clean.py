# Databricks notebook source
# MAGIC %md
# MAGIC # 01_customer_clean
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928141958-i75y/01_customer_clean.sas`
# COMMAND ----------
# COMMAND ----------
import dlt
from pyspark.sql.functions import col, when

# COMMAND ----------
def enriched_customers():
    # Read the raw source table (replace "raw_customers" with your actual source name)
    raw_df = dlt.read("raw_customers")

    # Apply the classification logic using Spark SQL expressions
    return raw_df.withColumn(
        "income_band",
        when(col("annual_income") < 20000, "LOW")
        .when(col("annual_income") < 80000, "MID")
        .otherwise("HIGH")
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
