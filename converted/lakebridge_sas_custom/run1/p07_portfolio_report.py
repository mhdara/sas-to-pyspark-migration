# Databricks notebook source
# MAGIC %md
# MAGIC # 07_portfolio_report
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260929024630-suw5/07_portfolio_report.sas`
# COMMAND ----------
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# ------------------------------------------------------------
# Include macro library (converted from the SAS %include)
# ------------------------------------------------------------
# MAGIC %run ./06_macro_library

# ------------------------------------------------------------
# Retrieve the HIGH_PRINCIPAL threshold from the control table
# ------------------------------------------------------------
df_params = spark.read.table("workspace.ctrl.macro_parameters")
high_principal_str = (
    df_params.filter(F.col("param_name") == "HIGH_PRINCIPAL")
    .select(F.col("param_value"))
    .first()[0]
)
# COMMAND ----------
# The value in the SAS table is stored as text; convert to numeric for comparisons
high_principal = float(high_principal_str)

# COMMAND ----------
# ------------------------------------------------------------
# Helper functions that replicate the SAS macros %summarize and %flag_high
# ------------------------------------------------------------
def summarize(ds: str, class_var: str, var: str, out: str):
    """
    Replicates %summarize:
      - Groups the input dataset by `class_var`
      - Calculates the sum of `var` for each group
      - Writes the result to the table named in `out`
    The output table contains:
      * class_var (as‑is)
      * total  – the summed value of `var`
    """
    df = spark.read.table(ds)
    df_out = (
        df.groupBy(F.col(class_var))
          .agg(F.sum(F.col(var)).alias("total"))
    )
    df_out.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(out)


# COMMAND ----------
def flag_high(ds: str, var: str, threshold: float, out: str):
    """
    Replicates %flag_high:
      - Reads the dataset `ds`
      - Creates a flag column (`high_flag`) that is 1 when `var` > `threshold`,
        otherwise 0 (missing values are treated as 0)
      - Writes the result to the table named in `out`
    The output table contains all original columns plus `high_flag`.
    """
    df = spark.read.table(ds)
    df_out = df.withColumn(
        "high_flag",
        F.when(F.col(var).isNull(), 0)
         .when(F.col(var) > F.lit(threshold), 1)
         .otherwise(0).cast("int")
    )
    df_out.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(out)

# COMMAND ----------
# ------------------------------------------------------------
# Execute the workflow described in the original SAS program
# ------------------------------------------------------------
# 1. Summarize loan principal per customer segment
summarize(
    ds="workspace.out.loan_enriched",
    class_var="segment_name",
    var="principal",
    out="workspace.out.portfolio_by_segment"
)

# COMMAND ----------
# 2. Flag segments whose total principal exceeds the HIGH_PRINCIPAL threshold
flag_high(
    ds="workspace.out.portfolio_by_segment",
    var="total",
    threshold=high_principal,
    out="workspace.out.portfolio_report"
)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
