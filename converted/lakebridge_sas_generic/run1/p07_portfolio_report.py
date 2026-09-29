# Databricks notebook source
# MAGIC %md
# MAGIC # 07_portfolio_report
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928145005-uq1l/07_portfolio_report.sas`
# COMMAND ----------
# COMMAND ----------
# --------------------------------------------------------------
# 07_portfolio_report.py  –  Databricks (PySpark) conversion
# --------------------------------------------------------------
# This script reproduces the SAS program that:
#   1. Reads a threshold value (HIGH_PRINCIPAL) from a control table.
#   2. Summarizes loan principal by customer segment.
#   3. Flags segments whose total principal exceeds the threshold.
# --------------------------------------------------------------

# ---------------------------------
# 1. Imports & session setup
# ---------------------------------
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# COMMAND ----------
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# ---------------------------------
# 2. Parameter / library path handling
# ---------------------------------
# In SAS the macro variable &root is used for library paths.
# In Databricks we retrieve it from a widget (or fallback to a default).
try:
    root = dbutils.widgets.get("root")          # e.g. "/Workspace/Repos/your_repo"
except Exception:
    root = "/mnt/data"                         # default placeholder

# COMMAND ----------
# Define fully‑qualified table identifiers (Unity Catalog or Hive metastore)
CTRL_TABLE   = f"{root}.ctrl.macro_parameters"   # ctrl.macro_parameters
LOAN_TABLE   = f"{root}.out.loan_enriched"       # out.loan_enriched
SEG_SUM_TAB  = f"{root}.out.portfolio_by_segment"   # out.portfolio_by_segment
REPORT_TAB   = f"{root}.out.portfolio_report"       # out.portfolio_report

# COMMAND ----------
# ---------------------------------
# 3. Retrieve the HIGH_PRINCIPAL threshold
# ---------------------------------
# SAS:  SELECT param_value INTO :high_principal FROM ctrl.macro_parameters
#       WHERE param_name='HIGH_PRINCIPAL';
high_principal_df = (
    spark.read.table(CTRL_TABLE)
    .filter(F.col("param_name") == "HIGH_PRINCIPAL")
    .select(F.col("param_value").alias("value"))
)

# COMMAND ----------
# If the parameter is missing, raise a clear error.
high_principal_value = high_principal_df.collect()
if not high_principal_value:
    raise ValueError("HIGH_PRINCIPAL parameter not found in ctrl.macro_parameters")
high_principal_str = high_principal_value[0]["value"]

# COMMAND ----------
# Convert the text value (e.g., '5000000') to a numeric type.
# SAS stores numbers as characters when read from a table, so we cast explicitly.
high_principal = float(high_principal_str.replace(",", ""))   # remove commas if present

# COMMAND ----------
# ---------------------------------
# 4. %summarize macro logic (loan principal per segment)
# ---------------------------------
# SAS macro %summarize(ds=out.loan_enriched, class=segment_name,
#                     var=principal, out=out.portfolio_by_segment);
#
# Equivalent PySpark:
loan_df = spark.read.table(LOAN_TABLE)

# COMMAND ----------
portfolio_by_segment_df = (
    loan_df
    .groupBy("segment_name")
    .agg(F.sum(F.col("principal")).alias("total"))
)

# COMMAND ----------
# Persist the intermediate result because it will be used in the next step.
portfolio_by_segment_df = portfolio_by_segment_df.cache()

# COMMAND ----------
# Write the summary table (equivalent to the SAS OUT= data set).
portfolio_by_segment_df.write.mode("overwrite").saveAsTable(SEG_SUM_TAB)

# COMMAND ----------
# ---------------------------------
# 5. %flag_high macro logic (apply threshold flag)
# ---------------------------------
# SAS macro %flag_high(ds=out.portfolio_by_segment, var=total,
#                     threshold=&high_principal, out=out.portfolio_report);
#
# Equivalent PySpark:
portfolio_report_df = (
    portfolio_by_segment_df
    .withColumn(
        "high_principal_flag",
        F.when(F.col("total") > high_principal, 1).otherwise(0)
    )
)

# COMMAND ----------
# Write the final report table (one row per segment, with the flag).
portfolio_report_df.write.mode("overwrite").saveAsTable(REPORT_TAB)

# COMMAND ----------
# ---------------------------------
# 6. Result preview
# ---------------------------------
display(portfolio_report_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
