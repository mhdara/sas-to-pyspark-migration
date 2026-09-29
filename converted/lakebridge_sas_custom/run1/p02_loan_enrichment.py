# Databricks notebook source
# MAGIC %md
# MAGIC # 02_loan_enrichment
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260929024630-suw5/02_loan_enrichment.sas`
# COMMAND ----------
# COMMAND ----------
# --------------------------------------------------------------
# 02_loan_enrichment.sas  -->  loan_enriched (Spark version)
# --------------------------------------------------------------
# This code reproduces the PROC SQL logic:
#   * inner join loans to cleaned customers (dropping loans with missing customers)
#   * left join the resulting rows to the client segment lookup
#   * keep all loan columns plus the selected customer/segment attributes
# --------------------------------------------------------------

import pyspark.sql.functions as F

# COMMAND ----------
# ------------------------------------------------------------------
# Read source tables from the Unity Catalog schemas
# ------------------------------------------------------------------
df_loans = spark.read.table("workspace.stg.loans")
df_customers = spark.read.table("workspace.out.customers_clean")
df_segments = spark.read.table("workspace.ctrl.client_segments")

# COMMAND ----------
# ------------------------------------------------------------------
# Join logic – exactly mirrors the SAS PROC SQL
#   1. inner join loans (l) to customers (c) on customer_id
#   2. left join the result to client_segments (s) on segment_code
# ------------------------------------------------------------------
df_joined = (
    df_loans.alias("l")
    .join(
        df_customers.alias("c"),
        on=F.col("l.customer_id") == F.col("c.customer_id"),
        how="inner",
    )
    .join(
        df_segments.alias("s"),
        on=F.col("c.segment_code") == F.col("s.segment_code"),
        how="left",
    )
)

# COMMAND ----------
# ------------------------------------------------------------------
# Select the exact columns that the SAS query outputs:
#   - all columns from the loan table (l.*)
#   - c.customer_name, c.province, c.income_band
#   - s.segment_name, s.risk_band
# Preserve the original SAS column names in lower‑case.
# ------------------------------------------------------------------
loan_cols = [F.col(f"l.{c}").alias(c.lower()) for c in df_loans.columns]

# COMMAND ----------
df_loan_enriched = df_joined.select(
    *loan_cols,
    F.col("c.customer_name").alias("customer_name"),
    F.col("c.province").alias("province"),
    F.col("c.income_band").alias("income_band"),
    F.col("s.segment_name").alias("segment_name"),
    F.col("s.risk_band").alias("risk_band"),
)

# COMMAND ----------
# ------------------------------------------------------------------
# Write the result to the target schema as a managed table
# ------------------------------------------------------------------
df_loan_enriched.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.out.loan_enriched")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
