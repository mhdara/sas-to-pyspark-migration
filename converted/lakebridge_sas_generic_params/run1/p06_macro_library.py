# Databricks notebook source
# MAGIC %md
# MAGIC # 06_macro_library
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928145005-uq1l/06_macro_library.sas`
# COMMAND ----------
# COMMAND ----------
# ==============================
# macros_library.py
# ==============================
# This module provides Python equivalents of the SAS macros defined in
# 06_macro_library.sas.  The functions use PySpark DataFrame operations and are
# intended for use in Databricks notebooks.

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from typing import List, Union


# COMMAND ----------
def summarize(
    df: DataFrame,
    class_cols: Union[str, List[str]],
    var: str,
    out_path: str = None,
) -> DataFrame:
    """
    Mimics the %summarize macro.

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame (equivalent to the SAS dataset passed via &ds).
    class_cols : str or list of str
        Column(s) used for grouping – corresponds to the SAS CLASS statement.
    var : str
        Name of the numeric variable to be aggregated – corresponds to the SAS VAR statement.
    out_path : str, optional
        If provided, the resulting DataFrame is written to this location
        (e.g., a Delta table path or a Unity Catalog table).  If omitted, the
        DataFrame is simply returned.

    Returns
    -------
    DataFrame
        A DataFrame with columns:
            - the CLASS column(s)
            - n   : count of non‑missing observations of `var`
            - total : sum of `var`
            - avg : average of `var`
    """
    # Ensure class_cols is a list for the groupBy API
    if isinstance(class_cols, str):
        class_cols = [class_cols]

    # Perform the aggregations
    agg_df = (
        df.groupBy(*class_cols)
        .agg(
            F.count(F.col(var)).alias("n"),
            F.sum(F.col(var)).alias("total"),
            F.avg(F.col(var)).alias("avg"),
        )
    )

    # Optionally write the result
    if out_path:
        # Write as a Delta table (adjust format if needed)
        agg_df.write.mode("overwrite").format("delta").save(out_path)

    return agg_df


# COMMAND ----------
def flag_high(
    df: DataFrame,
    var: str,
    threshold: float,
    out_path: str = None,
) -> DataFrame:
    """
    Mimics the %flag_high macro.

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame (equivalent to the SAS dataset passed via &ds).
    var : str
        Name of the numeric variable to compare against the threshold.
    threshold : float
        Numeric cutoff – rows where `var` > `threshold` receive a flag of 1.
        Missing values in `var` result in a flag of 0 (as in SAS).
    out_path : str, optional
        If provided, the resulting DataFrame is written to this location.
        If omitted, the DataFrame is simply returned.

    Returns
    -------
    DataFrame
        The original DataFrame plus a new column `high_flag` (integer 0/1).
    """
    flagged_df = df.withColumn(
        "high_flag",
        # In SAS, a missing value yields 0.  Spark returns null for the comparison,
        # so we coalesce to 0 after the when/otherwise logic.
        F.when(F.col(var) > F.lit(threshold), F.lit(1))
        .otherwise(F.lit(0))
        .cast("int"),
    )

    if out_path:
        flagged_df.write.mode("overwrite").format("delta").save(out_path)

    return flagged_df

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
