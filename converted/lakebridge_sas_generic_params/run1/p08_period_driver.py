# Databricks notebook source
# MAGIC %md
# MAGIC # 08_period_driver
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928145005-uq1l/08_period_driver.sas`
# COMMAND ----------
# COMMAND ----------
# --------------------------------------------------------------
# 08_period_driver.py  -  Databricks (PySpark) conversion
# Creates one payment summary per active reporting month.
# --------------------------------------------------------------

# ------------------------------------------------------------------
# 1. Setup & Imports
# ------------------------------------------------------------------
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# ------------------------------------------------------------------
# 2. Parameters & Library Paths
# ------------------------------------------------------------------
# In SAS the macro variable &root pointed to a base directory.
# In Databricks we expose the same value as a notebook widget.
# Example usage in a notebook cell before running this script:
#   dbutils.widgets.text("root", "/mnt/data")   # adjust as needed
#   dbutils.widgets.text("catalog", "my_catalog")   # Unity Catalog name
#   dbutils.widgets.text("ctrl_schema", "ctrl")
#   dbutils.widgets.text("out_schema", "out")
root_path    = dbutils.widgets.get("root")          # e.g. "/mnt/data"
catalog_name = dbutils.widgets.get("catalog")      # e.g. "my_catalog"
ctrl_schema  = dbutils.widgets.get("ctrl_schema")   # e.g. "ctrl"
out_schema   = dbutils.widgets.get("out_schema")    # e.g. "out"

# COMMAND ----------
# Helper to build fully‑qualified table names
def tbl(schema: str, name: str) -> str:
    """Return a Unity Catalog table reference <catalog>.<schema>.<name>."""
    return f"{catalog_name}.{schema}.{name}"

# COMMAND ----------
# ------------------------------------------------------------------
# 3. Read active reporting periods (equivalent to PROC SQL step)
# ------------------------------------------------------------------
# SELECT period_id INTO :period_list FROM ctrl.reporting_periods
# WHERE active_flag = 1 ORDER BY period_id;
periods_df = (
    spark.read.table(tbl(ctrl_schema, "reporting_periods"))
          .filter(F.col("active_flag") == 1)
          .select("period_id")
          .orderBy("period_id")
)

# COMMAND ----------
# Collect the list of period identifiers to the driver.
# NOTE: For very large lists you would stay in the distributed engine,
# but the macro original design expects a modest number of months.
period_list = [row["period_id"] for row in periods_df.collect()]

# COMMAND ----------
# ------------------------------------------------------------------
# 4. Define a reusable summarize function (replaces %summarize macro)
# ------------------------------------------------------------------
def summarize(df, class_col: str, var_col: str, out_table: str):
    """
    Produce total and average of `var_col` for each value of `class_col`,
    then write the result as a Delta table `out_table`.

    Parameters
    ----------
    df : DataFrame
        Source data containing at least `class_col` and `var_col`.
    class_col : str
        Column used for grouping (e.g., delinquent_flag).
    var_col : str
        Numeric column that will be summed and averaged (e.g., payment_amount).
    out_table : str
        Fully‑qualified target table name (catalog.schema.table).
    """
    summary_df = (
        df.groupBy(F.col(class_col))
          .agg(
              F.sum(F.col(var_col)).alias("total_payment"),
              F.avg(F.col(var_col)).alias("avg_payment")
          )
    )

    # Write/overwrite the summary table.  Delta is the default format in Databricks.
    (
        summary_df.write
                 .mode("overwrite")
                 .format("delta")
                 .saveAsTable(out_table)
    )
    # Optional: display in the notebook for quick verification
    display(summary_df)

# COMMAND ----------
# ------------------------------------------------------------------
# 5. Main loop – replicate the %period_driver macro
# ------------------------------------------------------------------
for pid in period_list:
    # --------------------------------------------------------------
    # 5.1  Extract payments for the current period (work.pay_<pid>)
    # --------------------------------------------------------------
    # DATA work.pay_&pid; SET out.delinquency; WHERE period_id = "&pid";
    pay_df = (
        spark.read.table(tbl(out_schema, "delinquency"))
              .filter(F.col("period_id") == pid)
    )

    # --------------------------------------------------------------
    # 5.2  Summarize payments (calls %summarize)
    # --------------------------------------------------------------
    # The output table name follows the pattern out.period_summary_<pid>
    out_table_name = tbl(out_schema, f"period_summary_{pid}")

    summarize(
        df=pay_df,
        class_col="delinquent_flag",
        var_col="payment_amount",
        out_table=out_table_name
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
