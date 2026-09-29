# Databricks notebook source
# MAGIC %md
# MAGIC # 08_period_driver
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260929024630-suw5/08_period_driver.sas`
# COMMAND ----------
# COMMAND ----------
# --------------------------------------------------------------
# 08_period_driver.py  –  Databricks (PySpark) conversion
# --------------------------------------------------------------
# This notebook reproduces the logic of 08_period_driver.sas:
#   1. Retrieve active reporting periods.
#   2. Loop over each period and invoke the %run_period macro logic.
#   3. For each period, filter the delinquency table and call the
#      %summarize macro (provided in the included macro library).
# --------------------------------------------------------------

import pyspark.sql.functions as F

# COMMAND ----------
# ---------- Include macro library ----------
# The original SAS code includes 06_macro_library.sas which defines
# the %summarize macro used below. In Databricks we include the
# corresponding notebook that contains the Python implementation.
# MAGIC %run ./06_macro_library

# ---------- 1. Get list of active periods ----------
# Read the reporting periods table from the ctrl schema.
df_periods = (
    spark.read.table("workspace.ctrl.reporting_periods")
    .filter(F.col("active_flag") == 1)          # active_flag = 1
    .orderBy(F.col("period_id"))                # order by period_id
)

# COMMAND ----------
# Collect the period identifiers into a Python list.
# The SAS macro builds a space‑separated list; we keep a Python list.
period_list = [row["period_id"] for row in df_periods.select("period_id").collect()]

# COMMAND ----------
# ---------- 2. Define helper that mimics %run_period ----------
def run_period(pid: str) -> None:
    """
    Replicates the SAS %run_period macro for a given period identifier.

    Parameters
    ----------
    pid : str
        The period identifier (e.g., '202509').

    Steps
    -----
    1. Read the delinquency table (out.delinquency).
    2. Keep only rows for the current period.
    3. Call the summarize routine (implemented in the included library)
       to create out.period_summary_<pid>.
    """
    # 1. Load delinquency data.
    df_pay = (
        spark.read.table("workspace.out.delinquency")
        .filter(F.col("period_id") == pid)      # WHERE period_id = "&pid"
    )

    # 2. Call the summarize macro.
    # The Python version of %summarize is expected to be defined in the
    # included 06_macro_library notebook as a function `summarize`.
    # Its signature mirrors the SAS macro:
    #   summarize(ds=DataFrame, class=column, var=column, out=table_name)
    summarize(
        ds=df_pay,
        class_col="delinquent_flag",
        var_col="payment_amount",
        out_table=f"workspace.out.period_summary_{pid}"
    )

# COMMAND ----------
# ---------- 3. Define driver that mimics %period_driver ----------
def period_driver() -> None:
    """
    Loops over all active periods (period_list) and runs run_period for each.
    """
    for pid in period_list:
        run_period(pid)

# COMMAND ----------
# ---------- 4. Execute the driver ----------
period_driver()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
