# Databricks notebook source
# MAGIC %md
# MAGIC # 07_portfolio_report
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928142931-up2j/07_portfolio_report.sas`
# COMMAND ----------
# COMMAND ----------
# ------------------------------------------------------------
# 07_portfolio_report.py  –  Databricks (PySpark) conversion
# ------------------------------------------------------------
# This script reproduces the logic of the original SAS program:
#   1. Reads the HIGH_PRINCIPAL threshold from ctrl.macro_parameters.
#   2. Summarises loan principal by customer segment (segment_name).
#   3. Flags segments whose total principal exceeds the threshold.
#   4. Writes two output tables:
#        - out.portfolio_by_segment  (segment totals)
#        - out.portfolio_report      (segment totals + high‑principal flag)
#
# NOTE: The original SAS code invoked macro definitions from an
# external file (06_macro_library.sas).  Because the macro bodies are
# not available in this context, their functionality has been
# re‑implemented directly with PySpark DataFrame operations.
# ------------------------------------------------------------

import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# ------------------------------------------------------------------
# 1. Runtime parameters / environment
# ------------------------------------------------------------------
# In SAS the macro libraries and paths were built from a macro variable
#   &root.  In Databricks we obtain the same value from a widget
#   (or you can set it manually).
# ------------------------------------------------------------------
try:
    root = dbutils.widgets.get("root")          # e.g. "/mnt/project"
except Exception:
    # Fallback for interactive testing – adjust as needed
    root = "/mnt/project"

# COMMAND ----------
# ------------------------------------------------------------------
# 2. Load the HIGH_PRINCIPAL threshold from the control table
# ------------------------------------------------------------------
# Table: ctrl.macro_parameters  (columns: param_name, param_value)
# The SAS code selected the value where param_name='HIGH_PRINCIPAL'
# and stored it as a macro variable (text).  Here we read it, cast to
# numeric, and keep it in a Python variable.
# ------------------------------------------------------------------
high_principal_df = (
    spark.read.table("ctrl.macro_parameters")
        .filter(F.col("param_name") == "HIGH_PRINCIPAL")
        .select(F.col("param_value"))
)

# COMMAND ----------
# The table is expected to contain a single row; raise an error if not.
high_principal_row = high_principal_df.limit(1).collect()
if not high_principal_row:
    raise ValueError("HIGH_PRINCIPAL parameter not found in ctrl.macro_parameters")

# COMMAND ----------
# Convert the stored string (e.g. "5000000") to a float.
high_principal = float(high_principal_row[0]["param_value"])

# COMMAND ----------
# ------------------------------------------------------------------
# 3. Summarise loan principal per segment  (%summarize)
# ------------------------------------------------------------------
# Input table: out.loan_enriched
#   - class variable: segment_name
#   - aggregation variable: principal
# Resulting table (out.portfolio_by_segment) will have:
#   segment_name, total (sum of principal)
# ------------------------------------------------------------------
loan_df = spark.read.table("out.loan_enriched")

# COMMAND ----------
portfolio_by_segment = (
    loan_df
        .groupBy("segment_name")
        .agg(F.sum(F.col("principal")).alias("total"))
        # Cache because the same result is used in the next step
        .cache()
)

# COMMAND ----------
# Write the intermediate result (optional – mimics the SAS OUT= dataset)
portfolio_by_segment.write.mode("overwrite").saveAsTable("out.portfolio_by_segment")

# COMMAND ----------
# ------------------------------------------------------------------
# 4. Flag high‑principal segments (%flag_high)
# ------------------------------------------------------------------
# The macro added a flag column when the segment total exceeded the
# threshold that was read in step 2.
# ------------------------------------------------------------------
portfolio_report = (
    portfolio_by_segment
        .withColumn(
            "high_principal_flag",
            # 1 = total > threshold, 0 otherwise
            F.when(F.col("total") > F.lit(high_principal), 1).otherwise(0)
        )
)

# COMMAND ----------
# Persist the final report table
portfolio_report.write.mode("overwrite").saveAsTable("out.portfolio_report")

# COMMAND ----------
# ------------------------------------------------------------------
# 5. Display the final report (Databricks visualisation)
# ------------------------------------------------------------------
display(portfolio_report)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
