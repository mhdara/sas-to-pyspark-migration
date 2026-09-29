# Databricks notebook source
# MAGIC %md
# MAGIC # 11_executive_report
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928142931-up2j/11_executive_report.sas`
# COMMAND ----------
# COMMAND ----------
# --------------------------------------------------------------
# Databricks‑optimized PySpark conversion of 11_executive_report.sas
# --------------------------------------------------------------
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# ------------------------------------------------------------------
# 1.  Define catalog / schema locations (equivalent to SAS libname)
# ------------------------------------------------------------------
# Assuming the Unity Catalog database "my_catalog" contains the
# ctrl and out schemas that correspond to the SAS libnames.
CTRL_SCHEMA = "my_catalog.ctrl"
OUT_SCHEMA  = "my_catalog.out"

# COMMAND ----------
# ------------------------------------------------------------------
# 2.  Read the DPD_THRESHOLD macro parameter from ctrl.macro_parameters
# ------------------------------------------------------------------
# In SAS the value is stored in a macro variable; here we fetch it
# into a Python variable.  Because the table is tiny we can collect
# the single row safely.
dpd_param_df = (
    spark.read.table(f"{CTRL_SCHEMA}.macro_parameters")
          .filter(F.col("param_name") == "DPD_THRESHOLD")
          .select(F.col("param_value"))
)

# COMMAND ----------
# If the parameter is missing we raise an informative error.
try:
    dpd_threshold = int(dpd_param_df.first()["param_value"])
except Exception as e:
    raise RuntimeError(
        "Unable to read DPD_THRESHOLD from ctrl.macro_parameters. "
        "Ensure the table exists and contains the parameter."
    ) from e

# COMMAND ----------
# ------------------------------------------------------------------
# 3.  Load source tables (out.loan_enriched, out.loan_status, out.fc,
#     out.monthly_repay)
# ------------------------------------------------------------------
df_loan_enriched = spark.read.table(f"{OUT_SCHEMA}.loan_enriched")
df_loan_status   = spark.read.table(f"{OUT_SCHEMA}.loan_status")
df_fc            = spark.read.table(f"{OUT_SCHEMA}.fc")
df_monthly_repay = spark.read.table(f"{OUT_SCHEMA}.monthly_repay")

# COMMAND ----------
# Cache tables that will be reused in multiple downstream steps.
df_loan_enriched.cache()
df_loan_status.cache()
df_fc.cache()
df_monthly_repay.cache()

# COMMAND ----------
# ------------------------------------------------------------------
# 4.  Create EXEC_REPORT
#    - one row per segment_name
#    - n_loans                 : distinct count of loan_id
#    - n_delinquent_loans      : count of loans where max_dpd > threshold
#    - total_principal         : sum of principal
# ------------------------------------------------------------------
# Join enriched loan data with status (left join, keep all enriched rows)
join_expr = df_loan_enriched["loan_id"] == df_loan_status["loan_id"]
df_report_join = df_loan_enriched.join(
    df_loan_status,
    on=join_expr,
    how="left"
)

# COMMAND ----------
# Aggregation per segment
df_exec_report = (
    df_report_join
    .groupBy("segment_name")
    .agg(
        F.countDistinct("loan_id").alias("n_loans"),
        F.sum(
            F.when(F.col("max_dpd") > dpd_threshold, 1).otherwise(0)
        ).alias("n_delinquent_loans"),
        F.sum("principal").alias("total_principal")
    )
)

# COMMAND ----------
# ------------------------------------------------------------------
# 5.  Create EXEC_FORECAST
#    - Keep only rows from out.fc where _type_ = 'FORECAST'
#    - Keep only future months (month > max(month) from monthly_repay)
# ------------------------------------------------------------------
# Determine the latest month present in the historical repayment file.
max_month_row = (
    df_monthly_repay.agg(F.max("month").alias("max_month")).first()
)
max_month = max_month_row["max_month"]

# COMMAND ----------
df_exec_forecast = (
    df_fc
    .filter(
        (F.col("_type_") == "FORECAST") &
        (F.col("month") > max_month)
    )
    .select(
        F.col("month"),
        F.col("total_payment").alias("forecast_payment")
    )
)

# COMMAND ----------
# ------------------------------------------------------------------
# 6.  Write results back to the OUT schema (equivalent to PROC SQL CREATE TABLE)
# ------------------------------------------------------------------
# Overwrite existing tables if they already exist.
df_exec_report.write.mode("overwrite").saveAsTable(f"{OUT_SCHEMA}.exec_report")
df_exec_forecast.write.mode("overwrite").saveAsTable(f"{OUT_SCHEMA}.exec_forecast")

# COMMAND ----------
# ------------------------------------------------------------------
# 7.  Display results for interactive inspection (Databricks notebooks)
# ------------------------------------------------------------------
display(df_exec_report)
display(df_exec_forecast)

# COMMAND ----------
# ------------------------------------------------------------------
# 8.  Cleanup: unpersist cached DataFrames (optional but good practice)
# ------------------------------------------------------------------
df_loan_enriched.unpersist()
df_loan_status.unpersist()
df_fc.unpersist()
df_monthly_repay.unpersist()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
