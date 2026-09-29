# Databricks notebook source
# MAGIC %md
# MAGIC # 02_loan_enrichment
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928145005-uq1l/02_loan_enrichment.sas`
# COMMAND ----------
# COMMAND ----------
# --------------------------------------------------------------
# 02_loan_enrichment conversion – SAS PROC SQL → PySpark DataFrames
# --------------------------------------------------------------

# --------------------------------------------------------------
# 1. Imports and optional macro handling
# --------------------------------------------------------------
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# COMMAND ----------
# If the SAS code uses a macro variable &root to point to a file system location,
# expose it as a Databricks widget (or set a default).  This example assumes the
# tables are registered in Unity Catalog, so we can reference them directly.
# Uncomment the following line if you need to retrieve the macro value:
# root_path = dbutils.widgets.get("root")   # e.g. "/mnt/data"

# --------------------------------------------------------------
# 2. Read source tables
# --------------------------------------------------------------
# In Databricks, treat the SAS libnames as schema names. Adjust the
# catalog/schema as needed for your environment.
loans_df          = spark.read.table("stg.loans")               # source loans
customers_df      = spark.read.table("out.customers_clean")    # cleaned customers
segments_df       = spark.read.table("ctrl.client_segments")   # segment lookup

# COMMAND ----------
# --------------------------------------------------------------
# 3. Perform the joins
# --------------------------------------------------------------
# - Inner join between loans and customers (drops loans without a matching
#   customer, replicating the SAS behavior).
# - Left join to the segment lookup (keeps all customers, adds segment info
#   when available).
enriched_df = (
    loans_df.alias("l")
    .join(
        customers_df.alias("c"),
        on=F.col("l.customer_id") == F.col("c.customer_id"),
        how="inner"
    )
    .join(
        segments_df.alias("s"),
        on=F.col("c.segment_code") == F.col("s.segment_code"),
        how="left"
    )
    # Select all loan columns plus the required enrichment fields.
    .select(
        "l.*",                                 # all columns from loans
        F.col("c.customer_name").alias("customer_name"),
        F.col("c.province").alias("province"),
        F.col("c.income_band").alias("income_band"),
        F.col("s.segment_name").alias("segment_name"),
        F.col("s.risk_band").alias("risk_band")
    )
)

# COMMAND ----------
# --------------------------------------------------------------
# 4. Write the result
# --------------------------------------------------------------
# The original SAS program creates a table OUT.LOAN_ENRICHED.
# In Databricks we can either register it as a temporary view or write
# it back to a managed table.  Here we create (or replace) a managed
# table in the `out` schema.
enriched_df.write.mode("overwrite").saveAsTable("out.loan_enriched")

# COMMAND ----------
# --------------------------------------------------------------
# 5. Display the final DataFrame (Databricks notebooks)
# --------------------------------------------------------------
display(enriched_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
