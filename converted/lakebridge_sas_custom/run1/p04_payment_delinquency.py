# Databricks notebook source
# MAGIC %md
# MAGIC # 04_payment_delinquency
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260929024630-suw5/04_payment_delinquency.sas`
# COMMAND ----------
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# ------------------------------------------------------------
# 1.  Retrieve the DPD threshold from the macro parameters table
# ------------------------------------------------------------
# The table ctrl.macro_parameters holds the value as text.
# We fetch the value for param_name = 'DPD_THRESHOLD' and cast it to integer.
dpd_threshold_row = (
    spark.read.table("workspace.ctrl.macro_parameters")
    .filter(F.col("param_name") == "DPD_THRESHOLD")
    .select(F.col("param_value"))
    .first()
)
# COMMAND ----------
# If the parameter is missing, default to a large number to avoid flagging any rows.
dpd_threshold = int(dpd_threshold_row[0]) if dpd_threshold_row else 9999

# COMMAND ----------
# ------------------------------------------------------------
# 2.  Read the raw payments and preserve the original order
# ------------------------------------------------------------
df_raw = (
    spark.read.table("workspace.stg.loan_payments")
    .withColumn("_ord", F.monotonically_increasing_id())   # keep input order for NODUPKEY
)

# COMMAND ----------
# ------------------------------------------------------------
# 3.  PROC SORT with NODUPKEY (keep the first row of each key in input order)
#    BY loan_id payment_date
# ------------------------------------------------------------
w_first = Window.partitionBy("loan_id", "payment_date").orderBy("_ord")
df_pay_sorted = (
    df_raw
    .withColumn("_rn", F.row_number().over(w_first))
    .filter(F.col("_rn") == 1)            # keep the first row per (loan_id, payment_date)
    .drop("_rn")
)

# COMMAND ----------
# ------------------------------------------------------------
# 4.  Generate cumulative fields, flags and period identifier
# ------------------------------------------------------------
# Window to compute running totals in BY loan_id order (payment_date then input order)
w_cum = (
    Window.partitionBy("loan_id")
    .orderBy("payment_date", "_ord")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

# COMMAND ----------
df_processed = (
    df_pay_sorted
    # Running total of payment_amount (missing treated as 0)
    .withColumn(
        "cum_paid",
        F.sum(F.coalesce(F.col("payment_amount"), F.lit(0))).over(w_cum)
    )
    # Running maximum of days_past_due (missing ignored, same as SAS MAX)
    .withColumn(
        "max_dpd",
        F.max(F.coalesce(F.col("days_past_due"), F.lit(0))).over(w_cum)
    )
    # Delinquent flag: 1 if days_past_due > threshold, else 0 (missing → 0)
    .withColumn(
        "delinquent_flag",
        F.when(F.col("days_past_due").isNull(), 0)
         .when(F.col("days_past_due") > dpd_threshold, 1)
         .otherwise(0)
    )
    # Period identifier in YYYYMM format
    .withColumn(
        "period_id",
        F.date_format(F.col("payment_date"), "yyyyMM")
    )
)

# COMMAND ----------
# ------------------------------------------------------------
# 5.  Split the result into the two output tables
#    - out.delinquency : one row per payment (all columns)
#    - out.loan_status  : one row per loan (last row after BY processing)
# ------------------------------------------------------------
# Identify the last row for each loan_id (same order as BY processing)
w_last = Window.partitionBy("loan_id").orderBy(F.desc("payment_date"), F.desc("_ord"))
df_with_last = df_processed.withColumn("_row_desc", F.row_number().over(w_last))

# COMMAND ----------
# Table out.loan_status: keep only loan_id, cum_paid, max_dpd from the last row
df_loan_status = (
    df_with_last
    .filter(F.col("_row_desc") == 1)
    .select(
        F.col("loan_id").alias("loan_id"),
        F.col("cum_paid").alias("cum_paid"),
        F.col("max_dpd").alias("max_dpd")
    )
)

# COMMAND ----------
# Table out.delinquency: all rows (remove the helper column)
df_delinquency = df_with_last.drop("_row_desc")

# COMMAND ----------
# ------------------------------------------------------------
# 6.  Write the output tables to the Unity Catalog schema `workspace.out`
# ------------------------------------------------------------
df_delinquency.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.out.delinquency")

# COMMAND ----------
df_loan_status.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.out.loan_status")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
