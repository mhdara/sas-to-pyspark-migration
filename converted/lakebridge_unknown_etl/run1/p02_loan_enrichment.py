# Databricks notebook source
# MAGIC %md
# MAGIC # 02_loan_enrichment
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928141958-i75y/02_loan_enrichment.sas`
# COMMAND ----------
# COMMAND ----------
import dlt
from pyspark.sql import functions as F

# COMMAND ----------
def stg_loans():
    """Stage table containing raw loan records."""
    return dlt.read("stg.loans")   # e.g. database.schema.table


# COMMAND ----------
def out_customers_clean():
    """Cleaned customer master data produced by the previous pipeline step."""
    return dlt.read("out.customers_clean")


# COMMAND ----------
def ctrl_client_segments():
    """Lookup table that maps segment codes to segment names and risk bands."""
    return dlt.read("ctrl.client_segments")


# COMMAND ----------
def loan_enriched():
    # Load source data as DataFrames
    loans_df   = dlt.read("stg_loans").alias("l")
    cust_df    = dlt.read("out_customers_clean").alias("c")
    segment_df = dlt.read("ctrl_client_segments").alias("s")

    # Perform the inner join with customers (drops loans missing a customer)
    joined_df = loans_df.join(
        cust_df,
        on=F.col("l.customer_id") == F.col("c.customer_id"),
        how="inner"
    )

    # Left join with the segment lookup (keeps all loan rows from the previous join)
    enriched_df = joined_df.join(
        segment_df,
        on=F.col("c.segment_code") == F.col("s.segment_code"),
        how="left"
    )

    # Select all original loan columns plus the enrichment fields
    result_df = enriched_df.select(
        "l.*",                         # all columns from the loan table
        F.col("c.customer_name"),     # additional customer attributes
        F.col("c.province"),
        F.col("c.income_band"),
        F.col("s.segment_name"),
        F.col("s.risk_band")
    )

    return result_df

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
