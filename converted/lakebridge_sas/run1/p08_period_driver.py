# Databricks notebook source
# MAGIC %md
# MAGIC # 08_period_driver
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928142931-up2j/08_period_driver.sas`
# COMMAND ----------
# COMMAND ----------
# --------------------------------------------------------------
# 08_period_driver.py  -  Databricks (PySpark) conversion
# --------------------------------------------------------------
# This notebook reproduces the logic of the SAS program
# 08_period_driver.sas which:
#   1) Reads the list of active reporting periods (e.g. 202509 … 202512)
#   2) For each period extracts that month’s payments from out.delinquency
#   3) Summarizes total and average payment by delinquent_flag
#   4) Writes one output table per period: out.period_summary_<period_id>
# --------------------------------------------------------------

# --------------------------------------------------------------
# 1. Imports & environment setup
# --------------------------------------------------------------
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# ----------------------------------------------------------------
# 2. Parameter handling (equivalent to SAS macro variables)
# ----------------------------------------------------------------
# In SAS the macro variable &root points to a base directory.
# In Databricks we expose the same value as a widget so that the
# notebook can be run with different roots without code changes.
# Example usage in a notebook cell before running the script:
#   dbutils.widgets.text("root", "/mnt/project")
#   dbutils.widgets.text("log_path", "/mnt/project/logs")
#   dbutils.widgets.text("program_path", "/mnt/project/programs")
#   dbutils.widgets.text("catalog", "my_catalog")   # optional, for Unity Catalog
#   dbutils.widgets.text("schema_ctrl", "ctrl")
#   dbutils.widgets.text("schema_out", "out")
# ----------------------------------------------------------------
root          = dbutils.widgets.get("root")          # e.g. "/mnt/project"
log_path      = dbutils.widgets.get("log_path")      # e.g. "/mnt/project/logs"
program_path  = dbutils.widgets.get("program_path")  # e.g. "/mnt/project/programs"
catalog       = dbutils.widgets.get("catalog")       # e.g. "my_catalog"
schema_ctrl   = dbutils.widgets.get("schema_ctrl")   # e.g. "ctrl"
schema_out    = dbutils.widgets.get("schema_out")    # e.g. "out"

# COMMAND ----------
# Helper to build fully‑qualified table names (Unity Catalog style)
def table_name(schema: str, name: str) -> str:
    """Return catalog.schema.table if catalog is defined, otherwise schema.table."""
    return f"{catalog}.{schema}.{name}" if catalog else f"{schema}.{name}"

# COMMAND ----------
# --------------------------------------------------------------
# 3. Read the list of active periods (equivalent to PROC SQL)
# --------------------------------------------------------------
# SAS: select period_id into :period_list separated by ' '
#       from ctrl.reporting_periods where active_flag = 1 order by period_id;
# --------------------------------------------------------------
periods_df = spark.read.table(table_name(schema_ctrl, "reporting_periods")) \
    .filter(F.col("active_flag") == 1) \
    .select(F.col("period_id")) \
    .orderBy("period_id")

# COMMAND ----------
# Collect the period IDs as strings (e.g. ['202509','202510',...])
period_list = [row["period_id"] for row in periods_df.collect()]

# COMMAND ----------
# --------------------------------------------------------------
# 4. Define the summarisation logic (the %summarize macro)
# --------------------------------------------------------------
def summarize_payments(df_payments, class_col: str, var_col: str):
    """
    Replicates the SAS %summarize macro used in program 06.
    Parameters
    ----------
    df_payments : DataFrame
        Payments for a single period.
    class_col : str
        Column used for classification (e.g., delinquent_flag).
    var_col : str
        Numeric variable to be summarised (e.g., payment_amount).

    Returns
    -------
    DataFrame
        One row per class value with total and average of var_col.
    """
    return (
        df_payments
        .groupBy(F.col(class_col))
        .agg(
            F.sum(F.col(var_col)).alias("total_payment"),
            F.avg(F.col(var_col)).alias("avg_payment")
        )
    )

# COMMAND ----------
# --------------------------------------------------------------
# 5. Loop over each active period and generate the summary tables
# --------------------------------------------------------------
# In SAS this is performed by the %period_driver macro which uses
# %scan and %do loops.  In Python we simply iterate over the list.
# --------------------------------------------------------------
for pid in period_list:
    try:
        # ------------------------------------------------------------------
        # 5.1 Extract payments for the current period (equivalent to DATA step)
        # ------------------------------------------------------------------
        # SAS: data work.pay_&pid; set out.delinquency; where period_id = "&pid";
        # ------------------------------------------------------------------
        df_pay = (
            spark.read.table(table_name(schema_out, "delinquency"))
            .filter(F.col("period_id") == pid)
        ).cache()   # cache because we reuse it for the summary

        # --------------------------------------------------------------
        # 5.2 Summarise payments by delinquent_flag (the %summarize macro)
        # --------------------------------------------------------------
        df_summary = summarize_payments(
            df_payments = df_pay,
            class_col    = "delinquent_flag",
            var_col      = "payment_amount"
        )

        # ------------------------------------------------------------------
        # 5.3 Write the summary to a period‑specific table
        #     (equivalent to out.period_summary_&pid)
        # ------------------------------------------------------------------
        out_table = table_name(schema_out, f"period_summary_{pid}")
        (
            df_summary
            .write
            .mode("overwrite")               # overwrite if the table already exists
            .format("delta")                  # store as Delta Lake (default in DBR)
            .saveAsTable(out_table)
        )

        # (Optional) Log success – in SAS this would be captured by MPRINT.
        print(f"Successfully created summary table: {out_table}")

    except Exception as e:
        # --------------------------------------------------------------
        # 5.4 Error handling – mirrors SAS's ABORT/ERROR behavior
        # --------------------------------------------------------------
        error_msg = f"Error processing period {pid}: {str(e)}"
        print(error_msg)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
