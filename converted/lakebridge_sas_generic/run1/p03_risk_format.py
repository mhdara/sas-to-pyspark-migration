# Databricks notebook source
# MAGIC %md
# MAGIC # 03_risk_format
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928145005-uq1l/03_risk_format.sas`
# COMMAND ----------
# COMMAND ----------
# --------------------------------------------------------------
# 03_risk_format.py  -  Recreate SAS PROC FORMAT logic in PySpark
# --------------------------------------------------------------

# --------------------------------------------------------------
# Setup
# --------------------------------------------------------------
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# Retrieve the SAS macro variable &root (passed as a Databricks widget)
# Example: dbutils.widgets.text("root", "/mnt/data")
root_path = dbutils.widgets.get("root")

# COMMAND ----------
# Define source and target locations (replace with your catalog/table names if needed)
# If the SAS libref "out" pointed to a folder, we mimic the same folder structure.
src_path = f"{root_path}/out/customers_clean"   # <- SAS: out.customers_clean
tgt_path = f"{root_path}/out/customer_risk"     # <- SAS: out.customer_risk

# COMMAND ----------
# --------------------------------------------------------------
# Read the cleaned customers data
# --------------------------------------------------------------
# Assuming the source data is stored as Delta (or Parquet). Adjust format if needed.
df_customers = spark.read.format("delta").load(src_path)

# COMMAND ----------
# --------------------------------------------------------------
# Re‑create the PROC FORMAT “incband”
#   low   -< 20000   ->  'LOW'
#   20000 -< 80000   ->  'MID'
#   80000 - high    ->  'HIGH'
#   other (including missing) -> 'UNKNOWN'
# --------------------------------------------------------------
df_risk = (
    df_customers
    # Create the income band column using vectorised conditions.
    .withColumn(
        "income_band_fmt",
        F.when(F.col("annual_income").isNull(), "UNKNOWN")
         .when(F.col("annual_income") < 20000, "LOW")
         .when((F.col("annual_income") >= 20000) & (F.col("annual_income") < 80000), "MID")
         .when(F.col("annual_income") >= 80000, "HIGH")
         .otherwise("UNKNOWN")                # catches any unexpected values
    )
)

# COMMAND ----------
# --------------------------------------------------------------
# Write the result to the target location
# --------------------------------------------------------------
# Overwrite if the target already exists (mirrors SAS “writes” behavior).
df_risk.write.mode("overwrite").format("delta").save(tgt_path)

# COMMAND ----------
# --------------------------------------------------------------
# Display the result (Databricks visualisation)
# --------------------------------------------------------------
display(df_risk)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
