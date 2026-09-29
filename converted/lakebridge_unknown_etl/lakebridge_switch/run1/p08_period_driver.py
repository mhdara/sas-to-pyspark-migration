# Databricks notebook source
# MAGIC %md
# MAGIC # 08_period_driver
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928141958-i75y/08_period_driver.sas`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline (DLT) equivalent of 08_period_driver.sas
# --------------------------------------------------------------
# This pipeline reproduces the SAS macro logic:
# 1. Reads reporting periods where active_flag = 1.
# 2. For each active period it filters the delinquency table to that period.
# 3. Summarises payment_amount by delinquent_flag (total and average).
# 4. Writes one output table per period named out.period_summary_<period_id>.
#
# The code uses the Python DLT API (PySpark) and creates the per‑period
# tables dynamically inside a Python loop.  The resulting tables are
# registered in the metastore with the same naming convention as the
# SAS version (out.period_summary_202509, out.period_summary_202510, …).

import dlt
from pyspark.sql import functions as f
from pyspark.sql import SparkSession

# COMMAND ----------
# ------------------------------------------------------------------
# Configuration – adjust these to match your environment.
# ------------------------------------------------------------------
ROOT_PATH = "/mnt/root"                     # base path that was &root in SAS
CTRL_DB   = "ctrl"                          # database/schema for control tables
OUT_DB    = "out"                           # database/schema for output tables
# COMMAND ----------
# The SAS libnames map to the following Spark catalogs / databases.
# It is assumed that Spark catalog entries `ctrl` and `out` have been
# created and point to the appropriate storage locations.

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# ------------------------------------------------------------------
# Helper: summarise payment amounts by delinquent_flag.
# This reproduces the behaviour of the %summarize macro from program 06.
# ------------------------------------------------------------------
def summarize_payments(df):
    """
    Input:  DataFrame containing at least the columns
            - delinquent_flag (string/int)
            - payment_amount   (numeric)
    Output: DataFrame with one row per delinquent_flag containing:
            - total_payment : sum(payment_amount)
            - avg_payment   : avg(payment_amount)
    """
    return (
        df.groupBy("delinquent_flag")
          .agg(
              f.sum("payment_amount").alias("total_payment"),
              f.avg("payment_amount").alias("avg_payment")
          )
    )

# COMMAND ----------
def active_reporting_periods():
    return (
        spark.table(f"{CTRL_DB}.reporting_periods")
             .filter(f.col("active_flag") == 1)
             .select("period_id")
             .orderBy("period_id")
    )

# COMMAND ----------
# ------------------------------------------------------------------
# Step 2 – dynamically create one DLT table per active period.
# The loop runs at pipeline compilation time, when the notebook/module is
# imported.  For each period we define a function, decorate it with @dlt.table,
# and give it a unique name that matches the SAS output convention.
# ------------------------------------------------------------------
# NOTE: DLT does not support "dynamic tables" at runtime, but defining the
# tables inside a Python loop works because the decorators are applied
# during module import (i.e., pipeline definition time).

# Collect the period identifiers now; this is a small lookup, so a driver‑side
# collect() is acceptable.
period_rows = (
    spark.table(f"{CTRL_DB}.reporting_periods")
         .filter(f.col("active_flag") == 1)
         .select(f.col("period_id").cast("string"))
         .orderBy("period_id")
         .collect()
)

# COMMAND ----------
for row in period_rows:
    pid = row["period_id"]                      # e.g., "202509"
    table_name = f"period_summary_{pid}"       # matches out.period_summary_202509 etc.

    # Define a function in the local scope that will be wrapped by the DLT decorator.
    # The function name must be unique; we embed the period id to guarantee uniqueness.
    def make_period_table(pid_value):
        @dlt.table(
            name=table_name,
            comment=f"Payment summary for reporting period {pid_value}",
            table_properties={"quality": "gold"}   # optional metadata
        )
        def _period_summary():
            # Load the full delinquency table (equivalent to out.delinquency in SAS)
            df = spark.table(f"{OUT_DB}.delinquency")
            # Keep only rows belonging to the current period.
            period_df = df.filter(f.col("period_id") == pid_value)
            # Apply the same aggregation logic as the SAS %summarize macro.
            summary_df = summarize_payments(period_df)
            return summary_df

        # Expose the function to the module globals so that DLT can discover it.
        # The name `_period_summary` is not important; DLT registers the table under
        # the `name=` argument supplied to @dlt.table.
        globals()[f"_period_summary_{pid_value}"] = _period_summary

    # Invoke the closure to create the table definition for this period.
    make_period_table(pid)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
