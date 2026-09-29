# Databricks notebook source
# MAGIC %md
# MAGIC # 11_executive_report
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928141958-i75y/11_executive_report.sas`
# COMMAND ----------
# COMMAND ----------
import dlt
from pyspark.sql import functions as F

# COMMAND ----------
def dpd_threshold_view():
    """
    Reads the DPD threshold from ctrl.macro_parameters.
    Returns a single-row DataFrame with column `dpd_threshold` (int).
    """
    return (
        spark.read.table("ctrl.macro_parameters")
        .filter(F.col("param_name") == "DPD_THRESHOLD")
        .select(F.col("param_value").cast("int").alias("dpd_threshold"))
        .limit(1)
    )

# COMMAND ----------
def exec_report():
    # Load source tables
    loan_enriched = spark.read.table("out.loan_enriched")
    loan_status   = spark.read.table("out.loan_status")
    threshold_df  = dlt.read("dpd_threshold_view")

    # Bring the threshold into the driver as a scalar value.
    # Since the view contains a single row, collect it safely.
    dpd_threshold = threshold_df.collect()[0]["dpd_threshold"]

    # Left join enriched loan info with status (may be missing for some loans)
    joined = (
        loan_enriched.alias("e")
        .join(
            loan_status.alias("s"),
            on=F.col("e.loan_id") == F.col("s.loan_id"),
            how="left"
        )
    )

    # Aggregation per segment
    report = (
        joined
        .groupBy(F.col("e.segment_name"))
        .agg(
            F.countDistinct(F.col("e.loan_id")).alias("n_loans"),
            # Delinquent loans: max_dpd > threshold
            F.sum(
                F.when(F.col("s.max_dpd") > dpd_threshold, 1).otherwise(0)
            ).alias("n_delinquent_loans"),
            F.sum(F.col("e.principal")).alias("total_principal")
        )
        .select(
            F.col("segment_name"),
            "n_loans",
            "n_delinquent_loans",
            "total_principal"
        )
    )

    return report

# COMMAND ----------
def exec_forecast():
    # Load source tables
    fc = spark.read.table("out.fc")
    monthly_repay = spark.read.table("out.monthly_repay")

    # Determine the maximum month that appears in the historical repayment table
    max_month_row = monthly_repay.agg(F.max(F.col("month")).alias("max_month")).collect()[0]
    max_month = max_month_row["max_month"]

    # Filter forecast rows: type = 'FORECAST' and month > max_month
    forecast = (
        fc.filter(
            (F.col("_type_") == "FORECAST") &
            (F.col("month") > max_month)
        )
        .select(
            F.col("month"),
            F.col("total_payment").alias("forecast_payment")
        )
    )

    return forecast
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
