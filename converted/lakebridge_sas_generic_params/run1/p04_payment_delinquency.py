# Databricks notebook source
# MAGIC %md
# MAGIC # 04_payment_delinquency
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928145005-uq1l/04_payment_delinquency.sas`
# COMMAND ----------
# COMMAND ----------
# --------------------------------------------------------------
# 04_payment_delinquency.py
# Databricks‑optimized PySpark conversion of 04_payment_delinquency.sas
# --------------------------------------------------------------

# --------------------------------------------------------------
# 1. Imports & helper objects
# --------------------------------------------------------------
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# --------------------------------------------------------------
# 2. Load macro parameter (DPD_THRESHOLD) from the control table
# --------------------------------------------------------------
# In SAS this value is stored as text.  We read it once and cast to integer.
dpd_threshold_df = (
    spark.read.table("ctrl.macro_parameters")
    .filter(F.col("param_name") == "DPD_THRESHOLD")
    .select(F.col("param_value"))
)

# COMMAND ----------
# If the table is guaranteed to have exactly one row, a simple collect() is fine.
dpd_threshold = int(dpd_threshold_df.first()["param_value"])

# COMMAND ----------
# --------------------------------------------------------------
# 3. Read the raw loan payments (equivalent to LIBNAME stg ... )
# --------------------------------------------------------------
df_payments = spark.read.table("stg.loan_payments")

# COMMAND ----------
# --------------------------------------------------------------
# 4. Sort payments by loan_id & payment_date and keep only the first
#    duplicate per (loan_id, payment_date) (SAS NODUPKEY keeps the first row)
# --------------------------------------------------------------
# Define a window that orders rows exactly as SAS would (by loan_id, payment_date)
w_duplicate = Window.partitionBy("loan_id", "payment_date").orderBy(F.monotonically_increasing_id())

# COMMAND ----------
df_sorted = (
    df_payments
    .withColumn("_row_num", F.row_number().over(w_duplicate))
    .filter(F.col("_row_num") == 1)                # keep the first occurrence only
    .drop("_row_num")
    .orderBy("loan_id", "payment_date")           # explicit ordering (optional for later windows)
)

# COMMAND ----------
# --------------------------------------------------------------
# 5. Compute running totals, maximum DPD, flag, and period identifier
# --------------------------------------------------------------
# Window for cumulative calculations per loan, ordered by payment_date
w_loan = Window.partitionBy("loan_id").orderBy("payment_date") \
             .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# COMMAND ----------
df_enhanced = (
    df_sorted
    # Cumulative amount paid (SAS sum statement)
    .withColumn("cum_paid", F.sum(F.col("payment_amount")).over(w_loan))
    # Maximum days past due seen so far (SAS RETAIN + MAX())
    .withColumn("max_dpd", F.max(F.col("days_past_due")).over(w_loan))
    # Late‑payment flag (days_past_due > threshold)
    .withColumn(
        "delinquent_flag",
        F.when(F.col("days_past_due") > dpd_threshold, True).otherwise(False)
    )
    # Period identifier in YYYYMM format (SAS PUT(date, yymmn6.))
    .withColumn("period_id", F.date_format(F.col("payment_date"), "yyyyMM"))
)

# COMMAND ----------
# --------------------------------------------------------------
# 6. Build the DELINQUENCY output (one row per payment)
# --------------------------------------------------------------
df_delinquency = df_enhanced.select(
    "*"
)   # keep all columns; adjust the column list if you need a subset

# COMMAND ----------
# --------------------------------------------------------------
# 7. Build the LOAN_STATUS output (one summary row per loan)
# --------------------------------------------------------------
# We need the final cumulative paid and the overall max DPD for each loan.
# The final cumulative paid is simply the maximum of the cum_paid column
# because it is monotonically increasing.
df_loan_status = (
    df_enhanced
    .groupBy("loan_id")
    .agg(
        F.max("cum_paid").alias("cum_paid"),
        F.max("max_dpd").alias("max_dpd")
    )
)

# COMMAND ----------
# --------------------------------------------------------------
# 8. Write results to the target schema (equivalent to LIBNAME out ...)
# --------------------------------------------------------------
# Replace `out` with the appropriate Unity Catalog path or DBFS location.
df_delinquency.write.mode("overwrite").saveAsTable("out.delinquency")
df_loan_status.write.mode("overwrite").saveAsTable("out.loan_status")

# COMMAND ----------
# --------------------------------------------------------------
# 9. Optional: cache if downstream steps reuse these DataFrames
# --------------------------------------------------------------
df_delinquency.cache()
df_loan_status.cache()

# COMMAND ----------
# --------------------------------------------------------------
# 10. Visual inspection (Databricks notebooks)
# --------------------------------------------------------------
display(df_delinquency)      # shows the detailed payment‑level rows
display(df_loan_status)      # shows one row per loan with cumulative totals

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
