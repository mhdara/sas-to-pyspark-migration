# Databricks notebook source
# MAGIC %md
# MAGIC # 01_customer_clean
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928142931-up2j/01_customer_clean.sas`
# COMMAND ----------
# COMMAND ----------
import pyspark.sql.functions as F

# COMMAND ----------
# Assuming `df` is the DataFrame that contains the column `annual_income`
df = df.withColumn(
    "income_band",
    F.when(F.col("annual_income") < 20000, "LOW")
     .when(F.col("annual_income") < 80000, "MID")
     .otherwise("HIGH")
)

# COMMAND ----------
# Show the result (or use `display(df)` in a Databricks notebook)
df.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
