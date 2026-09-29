# Databricks notebook source
# MAGIC %md
# MAGIC # 05_card_fraud_summary
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260929024630-suw5/05_card_fraud_summary.sas`
# COMMAND ----------
# COMMAND ----------
# ------------------------------------------------------------
# 05_card_fraud_summary.sas  -->  Databricks PySpark implementation
# ------------------------------------------------------------
# Reads   : workspace.stg.card_transactions
# Writes  : workspace.out.card_summary
#           workspace.out.fraud_freq
# ------------------------------------------------------------

import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# ------------------------------------------------------------------
# Load source data
# ------------------------------------------------------------------
df_txn = spark.read.table("workspace.stg.card_transactions")

# COMMAND ----------
# ------------------------------------------------------------------
# PROC MEANS equivalent
#   - class: merchant_category
#   - var  : amount
#   - output: n=n_txn, sum=total_amount, mean=avg_amount
#   - nway: keep only category rows (no overall total)
# ------------------------------------------------------------------
df_card_summary = (
    df_txn
    .groupBy("merchant_category")
    .agg(
        # SAS PROC MEANS N counts non‑missing values of the VAR
        F.count(F.col("amount")).alias("n_txn"),
        F.sum(F.col("amount")).alias("total_amount"),
        F.avg(F.col("amount")).alias("avg_amount")
    )
)

# COMMAND ----------
# Write the summary table
df_card_summary.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.out.card_summary")

# COMMAND ----------
# ------------------------------------------------------------------
# PROC FREQ equivalent
#   - tables merchant_category*fraud_flag / out=out.fraud_freq
#   - SAS excludes rows where either variable is missing
#   - Output includes COUNT and PERCENT (percent of the whole input)
# ------------------------------------------------------------------
# Remove rows with missing merchant_category or fraud_flag
df_freq_src = df_txn.filter(
    F.col("merchant_category").isNotNull() & F.col("fraud_flag").isNotNull()
)

# COMMAND ----------
# Total number of non‑missing observations (used for percent)
total_non_missing = df_freq_src.count()

# COMMAND ----------
# Frequency table (counts per combination)
df_fraud_freq = (
    df_freq_src
    .groupBy("merchant_category", "fraud_flag")
    .agg(F.count("*").alias("count"))
    .withColumn(
        "percent",
        # Percent of the whole input (multiply by 100, keep as double)
        (F.col("count") / F.lit(total_non_missing) * 100).cast("double")
    )
)

# COMMAND ----------
# Write the frequency table
df_fraud_freq.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.out.fraud_freq")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
