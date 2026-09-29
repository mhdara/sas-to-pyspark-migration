# Databricks notebook source
# MAGIC %md
# MAGIC # 05_card_fraud_summary
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928141958-i75y/05_card_fraud_summary.sas`
# COMMAND ----------
# COMMAND ----------
import dlt
from pyspark.sql.functions import col, count, sum as _sum, avg as _avg
from pyspark.sql import Window

# COMMAND ----------
def card_summary():
    # Read the staging table (assumed to be a Delta table registered in the catalog)
    src = dlt.read("card_transactions")

    # Aggregate by merchant_category
    return (
        src.groupBy("merchant_category")
           .agg(
               count("*").alias("n_txn"),
               _sum(col("amount")).alias("total_amount"),
               _avg(col("amount")).alias("avg_amount")
           )
    )

# COMMAND ----------
def fraud_freq():
    src = dlt.read("card_transactions")

    # Count rows per (merchant_category, fraud_flag) combination
    freq_df = (
        src.groupBy("merchant_category", "fraud_flag")
           .agg(count("*").alias("count"))
    )

    # Compute row‑wise percentage (percentage within each merchant_category)
    w = Window.partitionBy("merchant_category")
    freq_df = freq_df.withColumn(
        "percent",
        (col("count") / _sum(col("count")).over(w)) * 100
    )

    return freq_df
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
