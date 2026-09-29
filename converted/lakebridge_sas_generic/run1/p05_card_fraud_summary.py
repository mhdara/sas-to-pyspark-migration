# Databricks notebook source
# MAGIC %md
# MAGIC # 05_card_fraud_summary
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928145005-uq1l/05_card_fraud_summary.sas`
# COMMAND ----------
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# ----------------------------------------------------------------------
# Setup: retrieve the root path (if needed for file‑based tables) and import
# ----------------------------------------------------------------------
# In SAS the macro variable &root was used with LIBNAME statements.
# In Databricks we assume the tables are registered in Unity Catalog
# and can be referenced directly as `stg.card_transactions` and
# `out.card_summary` / `out.fraud_freq`.  If a file‑based location is
# required, uncomment the next line and adjust the path.
# root_path = dbutils.widgets.get("root")

# ----------------------------------------------------------------------
# Read the source transaction data
# ----------------------------------------------------------------------
df_txn = spark.read.table("stg.card_transactions")   # equivalent to LIBNAME stg &root/stg; SET stg.card_transactions;

# COMMAND ----------
# ----------------------------------------------------------------------
# PROC MEANS equivalent: compute count, sum, and mean of amount for each
# merchant_category (NWAY – only the class levels, no grand total)
# ----------------------------------------------------------------------
df_card_summary = (
    df_txn
    .groupBy("merchant_category")
    .agg(
        F.count("amount").alias("n_txn"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount")
    )
)

# COMMAND ----------
# Write the summary table (replace if it already exists)
df_card_summary.write.mode("overwrite").format("delta").saveAsTable("out.card_summary")

# COMMAND ----------
# ----------------------------------------------------------------------
# PROC FREQ equivalent: frequency of transactions by merchant_category
# and fraud_flag, including percentages.
# ----------------------------------------------------------------------
# 1. Count rows for each (merchant_category, fraud_flag) pair
df_freq_counts = (
    df_txn
    .groupBy("merchant_category", "fraud_flag")
    .agg(F.count("*").alias("count"))
)

# COMMAND ----------
# 2. Compute percentages within each merchant_category (optional – SAS PROC FREQ
#    provides row, column, and total percentages; here we provide the row percent)
window_cat = Window.partitionBy("merchant_category")
df_fraud_freq = (
    df_freq_counts
    .withColumn("total_by_category", F.sum("count").over(window_cat))
    .withColumn(
        "percent",
        F.round(F.col("count") / F.col("total_by_category") * 100, 2)
    )
    .drop("total_by_category")
)

# COMMAND ----------
# Write the frequency table
df_fraud_freq.write.mode("overwrite").format("delta").saveAsTable("out.fraud_freq")

# COMMAND ----------
# ----------------------------------------------------------------------
# Optional: display the results in a Databricks notebook
# ----------------------------------------------------------------------
display(df_card_summary)
display(df_fraud_freq)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
