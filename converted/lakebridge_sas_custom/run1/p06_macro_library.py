# Databricks notebook source
# MAGIC %md
# MAGIC # 06_macro_library
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260929024630-suw5/06_macro_library.sas`
# COMMAND ----------
# COMMAND ----------
# --------------------------------------------------------------
# 06_macro_library.py
# --------------------------------------------------------------
# This module contains Python equivalents of the SAS macros
# defined in 06_macro_library.sas.  The functions can be imported
# and used by other notebooks that previously %include the SAS file.
# --------------------------------------------------------------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# COMMAND ----------
spark = SparkSession.builder.getOrCreate()


# COMMAND ----------
def summarize(*, ds: str, class_: str | list, var: str, out: str) -> None:
    """
    Equivalent of the SAS macro %summarize.

    Parameters
    ----------
    ds : str
        Fully‑qualified table name to read (e.g. "workspace.stg.sales").
    class_ : str or list
        Column name(s) used for the CLASS statement.  Accepts a single
        column name, a whitespace‑separated string, or a list of strings.
    var : str
        The numeric variable to be aggregated.
    out : str
        Fully‑qualified table name to write the result to
        (e.g. "workspace.out.sales_summary").

    Behaviour
    ----------
    Performs a PROC MEANS‑style aggregation:
        - n   = count of non‑missing values of `var`
        - total = sum of `var` (missing values treated as 0)
        - avg = mean of `var` (missing values excluded)
    The resulting table contains only the CLASS columns and the three
    summary columns, matching the SAS output (the automatic _TYPE_ and
    _FREQ_ columns are not created).
    """
    # ------------------------------------------------------------------
    # Read source data
    # ------------------------------------------------------------------
    df = spark.read.table(ds)

    # ------------------------------------------------------------------
    # Prepare CLASS columns (allow a list or a space/comma separated string)
    # ------------------------------------------------------------------
    if isinstance(class_, str):
        # split on commas or whitespace
        class_cols = [c.strip() for c in class_.replace(",", " ").split() if c.strip()]
    else:
        class_cols = list(class_)

    # ------------------------------------------------------------------
    # Aggregation – SAS PROC MEANS NWAY means we want only the
    # highest‑level grouping (no _TYPE_ rows).  Using groupBy on the
    # CLASS columns reproduces this.
    # ------------------------------------------------------------------
    agg_df = (
        df.groupBy(*class_cols)
        .agg(
            F.count(F.col(var)).alias("n"),
            F.sum(F.coalesce(F.col(var), F.lit(0))).alias("total"),
            F.avg(F.col(var)).alias("avg"),
        )
        # Ensure column names are lower‑case as required by the project
        .toDF(*[c.lower() for c in class_cols] + ["n", "total", "avg"])
    )

    # ------------------------------------------------------------------
    # Write the result
    # ------------------------------------------------------------------
    agg_df.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(out)


# COMMAND ----------
def flag_high(*, ds: str, var: str, threshold: float, out: str) -> None:
    """
    Equivalent of the SAS macro %flag_high.

    Parameters
    ----------
    ds : str
        Fully‑qualified input table name.
    var : str
        Numeric variable to test against the threshold.
    threshold : float
        Threshold value; rows where `var` > threshold get high_flag = 1.
    out : str
        Fully‑qualified output table name.

    Behaviour
    ----------
    Creates a copy of the input data set with an additional column
    `high_flag` (integer 0/1).  In SAS a missing value results in 0;
    the same logic is reproduced here.
    """
    # ------------------------------------------------------------------
    # Read source data
    # ------------------------------------------------------------------
    df = spark.read.table(ds)

    # ------------------------------------------------------------------
    # Add the flag column – missing values become 0, otherwise 1/0
    # ------------------------------------------------------------------
    df_flagged = df.withColumn(
        "high_flag",
        F.when(F.col(var).isNull(), F.lit(0))
        .when(F.col(var) > F.lit(threshold), F.lit(1))
        .otherwise(F.lit(0))
    ).select(
        # Preserve original column order, then append the new flag column
        *df.columns,
        "high_flag"
    )

    # ------------------------------------------------------------------
    # Write the result
    # ------------------------------------------------------------------
    df_flagged.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(out)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
