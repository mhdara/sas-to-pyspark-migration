# Databricks notebook source
# MAGIC %md
# MAGIC # 05_card_fraud_summary
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928142931-up2j/05_card_fraud_summary.sas`
# COMMAND ----------
# COMMAND ----------
# ------------------------------------------------------------
# Setup
# ------------------------------------------------------------
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# If the SAS code uses macro variable &root, retrieve it from a Databricks widget
# (you can set this widget in the notebook before running the conversion).
root = dbutils.widgets.get("root")  # e.g., "/mnt/project"

# COMMAND ----------
# Define the fully‑qualified table names (Unity Catalog) or paths.
# Adjust the catalog/schema names if your environment differs.
stg_tbl   = f"{root}.stg.card_transactions"   # source table
out_schema = f"{root}.out"                    # destination schema (catalog)

# COMMAND ----------
# ------------------------------------------------------------
# Read source data
# ------------------------------------------------------------
df_txn = spark.read.table(stg_tbl)

# COMMAND ----------
# ------------------------------------------------------------
# 1) Card spending summary per merchant category
#    (equivalent to PROC MEANS with CLASS merchant_category)
# ------------------------------------------------------------
df_summary = (
    df_txn
    .groupBy("merchant_category")
    .agg(
        F.count("*").alias("n_txn"),               # number of transactions
        F.sum("amount").alias("total_amount"),      # total amount (includes negatives & zeros)
        F.avg("amount").alias("avg_amount")        # average amount
    )
)

# COMMAND ----------
# Write the summary table (drop SAS‑specific _TYPE_ and _FREQ_ variables)
(df_summary
 .write
 .mode("overwrite")                     # overwrite if the table already exists
 .saveAsTable(f"{out_schema}.card_summary")
)

# COMMAND ----------
# ------------------------------------------------------------
# 2) Frequency table of merchant_category * fraud_flag
#    (equivalent to PROC FREQ)
# ------------------------------------------------------------
df_fraud_freq = (
    df_txn
    .groupBy("merchant_category", "fraud_flag")
    .agg(F.count("*").alias("freq"))      # count of transactions for each combination
)

# COMMAND ----------
# Write the frequency table
(df_fraud_freq
 .write
 .mode("overwrite")
 .saveAsTable(f"{out_schema}.fraud_freq")
)

# COMMAND ----------
# ------------------------------------------------------------
# Optional: show results in a Databricks notebook
# ------------------------------------------------------------
display(df_summary)      # one row per merchant_category
display(df_fraud_freq)   # merchant_category × fraud_flag counts

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
