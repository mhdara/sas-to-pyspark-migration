# Databricks notebook source
# MAGIC %md
# MAGIC # 03_risk_format
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260929024630-suw5/03_risk_format.sas`
# COMMAND ----------
# COMMAND ----------
import pyspark.sql.functions as F

# COMMAND ----------
# ------------------------------------------------------------
# 03_risk_format.sas  ->  Python conversion
# ------------------------------------------------------------
# Reads  : workspace.out.customers_clean
# Writes : workspace.out.customer_risk
# Purpose: Create a categorical label (income_band_fmt) based on
#          annual_income using the same range boundaries as the
#          SAS PROC FORMAT "incband".
#          - income < 20,000          -> 'LOW'
#          - 20,000 <= income < 80,000 -> 'MID'
#          - income >= 80,000          -> 'HIGH'
#          - missing income            -> 'UNKNOWN' (SAS OTHER catches missing)
# ------------------------------------------------------------

# Load source table
df_customers = spark.read.table("workspace.out.customers_clean")

# COMMAND ----------
# Apply the format logic using Spark column expressions
df_risk = df_customers.withColumn(
    "income_band_fmt",
    F.when(F.col("annual_income").isNull(), "UNKNOWN")
     .when(F.col("annual_income") < 20000, "LOW")
     .when(F.col("annual_income") < 80000, "MID")
     .otherwise("HIGH")
)

# COMMAND ----------
# Write the result to the output table
df_risk.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.out.customer_risk")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
