# Databricks notebook source
# MAGIC %md
# MAGIC # 11_executive_report
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928145005-uq1l/11_executive_report.sas`
# COMMAND ----------
# COMMAND ----------
# --------------------------------------------------------------
# 11_executive_report.py  -  Databricks (PySpark) conversion
# --------------------------------------------------------------
# This script reproduces the logic of 11_executive_report.sas.
# It:
#   1. Reads the DPD_THRESHOLD macro parameter from the control table.
#   2. Generates a one‑row‑per‑segment executive report (count of loans,
#      count of delinquent loans, total principal).
#   3. Produces a 12‑month future repayment forecast.
# --------------------------------------------------------------

# --------------------------------------------------------------
# 1.  Imports & environment setup
# --------------------------------------------------------------
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# Optional: retrieve a root folder path passed as a widget (similar to SAS macro variable &root)
# In a notebook you could create a widget with: dbutils.widgets.text("root","/mnt/data")
root_path = dbutils.widgets.get("root") if "root" in dbutils.widgets.getArgumentNames() else ""

# COMMAND ----------
# --------------------------------------------------------------
# 2.  Load control parameter DPD_THRESHOLD
# --------------------------------------------------------------
# The SAS code reads the value into a macro variable. Here we pull the value
# into a Python variable so it can be used in later expressions.
dpd_df = spark.read.table(f"{root_path}/ctrl.macro_parameters") \
                .filter(F.col("param_name") == "DPD_THRESHOLD") \
                .select(F.col("param_value").cast("int").alias("dpd_threshold"))

# COMMAND ----------
# Collect the single value to the driver.  If the table is empty we default to 30
dpd_row = dpd_df.first()
dpd_threshold = dpd_row.dpd_threshold if dpd_row else 30

# COMMAND ----------
# --------------------------------------------------------------
# 3.  Read source tables
# --------------------------------------------------------------
loan_enriched_df   = spark.read.table(f"{root_path}/out.loan_enriched")   # e
loan_status_df     = spark.read.table(f"{root_path}/out.loan_status")     # s
fc_df              = spark.read.table(f"{root_path}/out.fc")              # forecast source
monthly_repay_df   = spark.read.table(f"{root_path}/out.monthly_repay")   # for max month lookup

# COMMAND ----------
# --------------------------------------------------------------
# 4.  Create EXEC_REPORT (one row per segment)
# --------------------------------------------------------------
# Left join loan_enriched with loan_status on loan_id
joined_df = loan_enriched_df.alias("e") \
    .join(loan_status_df.alias("s"),
          on=F.col("e.loan_id") == F.col("s.loan_id"),
          how="left") \
    .select(
        F.col("e.segment_name"),
        F.col("e.loan_id"),
        F.col("e.principal"),
        F.col("s.max_dpd")
    )

# COMMAND ----------
# Build the aggregation
exec_report_df = joined_df.groupBy("segment_name").agg(
    F.countDistinct("loan_id").alias("n_loans"),
    # Count delinquent loans: max_dpd > dpd_threshold
    F.sum(
        F.when(F.col("max_dpd") > dpd_threshold, 1).otherwise(0)
    ).alias("n_delinquent_loans"),
    F.sum("principal").alias("total_principal")
)

# COMMAND ----------
# --------------------------------------------------------------
# 5.  Create EXEC_FORECAST (future months only)
# --------------------------------------------------------------
# Determine the latest month that appears in the monthly_repay table
max_month_row = monthly_repay_df.agg(F.max("month").alias("max_month")).first()
max_month = max_month_row.max_month if max_month_row else None

# COMMAND ----------
# Filter the forecast table for rows with _type_ = 'FORECAST' and month > max_month
exec_forecast_df = fc_df.filter(
    (F.col("_type_") == "FORECAST") &
    (F.col("month") > F.lit(max_month))
).select(
    F.col("month"),
    F.col("total_payment").alias("forecast_payment")
)

# COMMAND ----------
# --------------------------------------------------------------
# 6.  Persist results
# --------------------------------------------------------------
# Overwrite (or create) the output tables in the `out` schema.
exec_report_df.write.mode("overwrite").saveAsTable(f"{root_path}/out.exec_report")
exec_forecast_df.write.mode("overwrite").saveAsTable(f"{root_path}/out.exec_forecast")

# COMMAND ----------
# --------------------------------------------------------------
# 7.  Display results (Databricks notebooks)
# --------------------------------------------------------------
display(exec_report_df)
display(exec_forecast_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
