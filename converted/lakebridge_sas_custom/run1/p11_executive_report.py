# Databricks notebook source
# MAGIC %md
# MAGIC # 11_executive_report
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260929024630-suw5/11_executive_report.sas`
# COMMAND ----------
# COMMAND ----------
# --------------------------------------------------------------
# 11_executive_report.sas  ->  Databricks PySpark conversion
# --------------------------------------------------------------

import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# ------------------------------------------------------------------
# 1. Read macro parameter DPD_THRESHOLD from the control table
# ------------------------------------------------------------------
# The SAS code loads the value into a macro variable. Here we retrieve it
# as a Python scalar (int) to use later in the DataFrame expressions.
dpd_row = (
    spark.read.table("workspace.ctrl.macro_parameters")
    .filter(F.col("param_name") == "DPD_THRESHOLD")
    .select(F.col("param_value"))
    .first()
)
# COMMAND ----------
# If the parameter is missing the code would have failed in SAS; we follow suit.
dpd_threshold = int(dpd_row["param_value"]) if dpd_row is not None else None

# COMMAND ----------
# ------------------------------------------------------------------
# 2. Build the executive report (one row per segment)
# ------------------------------------------------------------------
# Read the enriched loan data and the loan status data.
df_enriched = spark.read.table("workspace.out.loan_enriched").alias("e")
df_status   = spark.read.table("workspace.out.loan_status").alias("s")

# COMMAND ----------
# Left join on loan_id.
df_joined = df_enriched.join(
    df_status,
    on=F.col("e.loan_id") == F.col("s.loan_id"),
    how="left"
)

# COMMAND ----------
# Compute the boolean flag (max_dpd > threshold) as 1/0, treating missing as 0.
# In SAS, a missing number makes the comparison false (0).
delinquent_flag = F.when(F.col("s.max_dpd").isNull(), 0) \
                    .when(F.col("s.max_dpd") > dpd_threshold, 1) \
                    .otherwise(0)

# COMMAND ----------
# Aggregate per segment.
df_exec_report = (
    df_joined.groupBy("e.segment_name")
    .agg(
        F.countDistinct("e.loan_id").alias("n_loans"),
        F.sum(delinquent_flag).alias("n_delinquent_loans"),
        F.sum(F.col("e.principal")).alias("total_principal")
    )
    # Ensure column names are lower‑case as required.
    .select(
        F.col("segment_name").alias("segment_name"),
        F.col("n_loans").alias("n_loans"),
        F.col("n_delinquent_loans").alias("n_delinquent_loans"),
        F.col("total_principal").alias("total_principal")
    )
)

# COMMAND ----------
# Write the report to the output schema.
df_exec_report.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.out.exec_report")

# COMMAND ----------
# ------------------------------------------------------------------
# 3. Build the executive forecast (future months only)
# ------------------------------------------------------------------
# Read the forecast table and the monthly repayment table.
df_fc          = spark.read.table("workspace.out.fc").alias("fc")
df_monthly_rep = spark.read.table("workspace.out.monthly_repay").alias("mr")

# COMMAND ----------
# Determine the last month that appears in the historical repayment data.
max_month_row = df_monthly_rep.agg(F.max(F.col("month")).alias("max_month")).first()
max_month = max_month_row["max_month"]  # keep the same data type (date/int)

# COMMAND ----------
# Filter the forecast rows: keep only rows with _type_ = 'FORECAST' and month > max_month.
df_exec_forecast = (
    df_fc.filter(
        (F.col("_type_") == "FORECAST") &
        (F.col("month") > max_month)
    )
    .select(
        F.col("month").alias("month"),
        F.col("total_payment").alias("forecast_payment")
    )
)

# COMMAND ----------
# Write the forecast table.
df_exec_forecast.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.out.exec_forecast")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
