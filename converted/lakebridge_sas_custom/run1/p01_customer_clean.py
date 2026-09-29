# Databricks notebook source
# MAGIC %md
# MAGIC # 01_customer_clean
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260929024630-suw5/01_customer_clean.sas`
# COMMAND ----------
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# -------------------------------------------------------------------------
# 01_customer_clean.sas  ->  Databricks PySpark version
# -------------------------------------------------------------------------
# Reads  : workspace.stg.customers
# Writes : workspace.out.customers_clean
# -------------------------------------------------------------------------

# Load source table
df = spark.read.table("workspace.stg.customers")

# COMMAND ----------
# Apply the transformations described in the SAS DATA step
df_clean = (
    df
    # Remove leading/trailing blanks from CUSTOMER_NAME
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    # Upper‑case the PROVINCE code
    .withColumn("province", F.upper(F.col("province")))
    # Flag missing annual_income (0 = not missing, 1 = missing)
    .withColumn(
        "income_missing",
        F.when(F.col("annual_income").isNull(), F.lit(1)).otherwise(F.lit(0))
    )
    # Assign INCOME_BAND respecting the SAS rule that a missing value is
    # considered lower than any number (i.e., it falls into the LOW band)
    .withColumn(
        "income_band",
        F.when(
            F.col("annual_income").isNull() | (F.col("annual_income") < 20000),
            F.lit("LOW")
        )
        .when(F.col("annual_income") < 80000, F.lit("MID"))
        .otherwise(F.lit("HIGH"))
    )
)

# COMMAND ----------
# Write the result to the target Unity Catalog schema
df_clean.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.out.customers_clean")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
