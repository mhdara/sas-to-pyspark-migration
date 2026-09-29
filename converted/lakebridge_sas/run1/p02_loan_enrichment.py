# Databricks notebook source
# MAGIC %md
# MAGIC # 02_loan_enrichment
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928142931-up2j/02_loan_enrichment.sas`
# COMMAND ----------
# COMMAND ----------
# ------------------------------------------------------------
# Setup – import Spark functions and window utilities
# ------------------------------------------------------------
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# ------------------------------------------------------------
# Parameters (replace with appropriate widget or hard‑code)
# ------------------------------------------------------------
# In the original SAS code the macro variable &ROOT points to a base path.
# In Databricks we can expose it as a widget, e.g.:
# dbutils.widgets.text("root", "/mnt/project")
root = dbutils.widgets.get("root")  # <-- adjust as needed

# COMMAND ----------
# ------------------------------------------------------------
# Library (libname) mappings
# ------------------------------------------------------------
# SAS: libname stg "&root/stg";
# SAS: libname ctrl "&root/ctrl";
# SAS: libname out  "&root/out";
# In Databricks we reference tables directly via Unity Catalog or
# external locations.  Assuming the tables are registered in the
# catalog `my_catalog`, the paths become:
stg_schema   = f"{root}.stg"    # e.g. "my_catalog.stg"
ctrl_schema  = f"{root}.ctrl"
out_schema   = f"{root}.out"

# COMMAND ----------
# ------------------------------------------------------------
# Read source tables
# ------------------------------------------------------------
loans_df          = spark.read.table(f"{stg_schema}.loans")
customers_df      = spark.read.table(f"{out_schema}.customers_clean")
client_segments_df = spark.read.table(f"{ctrl_schema}.client_segments")

# COMMAND ----------
# ------------------------------------------------------------
# Join logic (direct translation of PROC SQL)
# ------------------------------------------------------------
# 1. Inner join loans → customers (drops loans without a matching customer)
# 2. Left join customers → client_segments (keeps all loans even if segment missing)
enriched_df = (
    loans_df.alias("l")
    .join(
        customers_df.alias("c"),
        on=F.col("l.customer_id") == F.col("c.customer_id"),
        how="inner"          # matches PROC SQL inner join
    )
    .join(
        client_segments_df.alias("s"),
        on=F.col("c.segment_code") == F.col("s.segment_code"),
        how="left"           # matches PROC SQL left join
    )
    # Select all columns from loans (l.*) plus the requested lookup fields
    .select(
        "l.*",                         # all loan columns
        F.col("c.customer_name").alias("customer_name"),
        F.col("c.province").alias("province"),
        F.col("c.income_band").alias("income_band"),
        F.col("s.segment_name").alias("segment_name"),
        F.col("s.risk_band").alias("risk_band")
    )
)

# COMMAND ----------
# ------------------------------------------------------------
# (Optional) Cache if the DataFrame is reused later
# ------------------------------------------------------------
enriched_df = enriched_df.cache()

# COMMAND ----------
# ------------------------------------------------------------
# Write the result to the target table (equivalent to CREATE TABLE)
# ------------------------------------------------------------
# The SAS code creates OUT.LOAN_ENRICHED.  We will overwrite or
# create the table with the same fully‑qualified name.
(
    enriched_df
    .write
    .mode("overwrite")                     # matches CREATE TABLE behavior
    .saveAsTable(f"{out_schema}.loan_enriched")
)

# COMMAND ----------
# ------------------------------------------------------------
# Display the enriched data (Databricks UI)
# ------------------------------------------------------------
display(enriched_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
