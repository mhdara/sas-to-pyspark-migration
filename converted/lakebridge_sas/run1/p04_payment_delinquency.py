# Databricks notebook source
# MAGIC %md
# MAGIC # 04_payment_delinquency
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928142931-up2j/04_payment_delinquency.sas`
# COMMAND ----------
# COMMAND ----------
# ------------------------------------------------------------
# Databricks PySpark conversion of the SAS DATA step logic
# ------------------------------------------------------------
# Imports
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# -----------------------------------------------------------------
# Parameters (equivalent to SAS macro variables)
# -----------------------------------------------------------------
# Example: &dpd_threshold is passed as a Databricks widget
dpd_threshold = int(dbutils.widgets.get("dpd_threshold"))  # e.g., 30

# COMMAND ----------
# -----------------------------------------------------------------
# Source DataFrame
# -----------------------------------------------------------------
# Replace the table name with the actual source location that contains
# the columns: loan_id, days_past_due, payment_date, etc.
df_src = spark.read.table("mydata.loan_payments")

# COMMAND ----------
# -----------------------------------------------------------------
# 1. Calculate running maximum of days past due per loan (max_dpd)
# -----------------------------------------------------------------
# Define a window that orders rows within each loan (assumes payment_date
# reflects the natural order; adjust if another ordering column exists)
w_loan = Window.partitionBy("loan_id").orderBy("payment_date") \
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# COMMAND ----------
df_with_max = df_src.withColumn(
    "max_dpd",
    F.greatest(
        # Preserve any existing max_dpd from previous rows (if present)
        F.coalesce(F.col("max_dpd"), F.lit(0)),
        F.col("days_past_due")
    )
).withColumn(
    # Cumulative maximum using the window
    "max_dpd",
    F.max("max_dpd").over(w_loan)
)

# COMMAND ----------
# -----------------------------------------------------------------
# 2. Delinquent flag based on the threshold macro variable
# -----------------------------------------------------------------
df_with_flag = df_with_max.withColumn(
    "delinquent_flag",
    (F.col("days_past_due") > F.lit(dpd_threshold)).cast("int")
)

# COMMAND ----------
# -----------------------------------------------------------------
# 3. Period identifier – SAS format yymmn6. -> two‑digit year + two‑digit month
# -----------------------------------------------------------------
# SAS yymmn6. results in YYMM (e.g., 2307 for July 2023).  Spark's
# date_format "yyMM" gives the same 4‑character string; prepend a leading
# zero if a 6‑character format is required (e.g., 202307).  Here we follow
# the 6‑character representation (YYYYMM) which is the most common usage.
df_with_period = df_with_flag.withColumn(
    "period_id",
    F.date_format(F.col("payment_date"), "yyyyMM")   # results in YYYYMM (6 chars)
)

# COMMAND ----------
# -----------------------------------------------------------------
# 4. Output DataFrames
# -----------------------------------------------------------------
# a) Delinquency dataset – every processed row
df_delinquency = df_with_period.select(
    "*"
)

# COMMAND ----------
# b) Loan status dataset – only the last record for each loan_id
# Determine the last row per loan using row_number on descending order
w_last = Window.partitionBy("loan_id").orderBy(F.col("payment_date").desc())
df_loan_status = df_with_period \
    .withColumn("row_num", F.row_number().over(w_last)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

# COMMAND ----------
# -----------------------------------------------------------------
# Optional caching if downstream steps reuse the results
# -----------------------------------------------------------------
df_delinquency.cache()
df_loan_status.cache()

# COMMAND ----------
# -----------------------------------------------------------------
# Display results in Databricks notebooks
# -----------------------------------------------------------------
display(df_delinquency)      # equivalent to SAS OUTPUT out.delinquency
display(df_loan_status)      # equivalent to SAS OUTPUT out.loan_status (last.loan_id)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
