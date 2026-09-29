# Databricks notebook source
# MAGIC %md
# MAGIC # 07_portfolio_report
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928141958-i75y/07_portfolio_report.sas`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline (Python)  
# This pipeline reproduces the logic of the SAS program 07_portfolio_report.sas.  
# It:  
# 1. Reads the HIGH_PRINCIPAL threshold from the ctrl.macro_parameters table.  
# 2. Summarises loan principal by customer segment (segment_name).  
# 3. Flags segments whose total principal exceeds the threshold.  

import dlt
from pyspark.sql import functions as F

# COMMAND ----------
def high_principal_config():
    # Read the parameters table from the "ctrl" schema / location
    params = dlt.read("macro_parameters")  # expects a Delta table named "macro_parameters"
    # Filter for the HIGH_PRINCIPAL row and cast the value to a numeric type
    high_principal = (
        params.filter(F.col("param_name") == "HIGH_PRINCIPAL")
              .select(F.col("param_value").alias("high_principal_str"))
              .limit(1)
    )
    # Cast the text value to a double (e.g., "5000000" -> 5000000.0)
    return high_principal.withColumn(
        "high_principal", F.col("high_principal_str").cast("double")
    ).drop("high_principal_str")


# COMMAND ----------
def loan_enriched():
    # The source path / table name should match the original out.loan_enriched location.
    # Adjust the format / location as needed for your environment.
    return dlt.read("loan_enriched")  # expects a Delta table called "loan_enriched"


# COMMAND ----------
def portfolio_by_segment():
    return (
        dlt.read("loan_enriched")
        .groupBy("segment_name")
        .agg(F.sum(F.col("principal")).alias("total_principal"))
    )


# COMMAND ----------
def portfolio_report():
    # Load the summarised data
    portfolio = dlt.read("portfolio_by_segment")

    # Load the threshold (single row dataframe with column high_principal)
    threshold_df = dlt.read("high_principal_config")

    # Broadcast the threshold value so it can be used in the expression
    # (alternatively collect it to driver – here we use a cross join for simplicity)
    flagged = (
        portfolio.crossJoin(threshold_df)
        .withColumn(
            "high_portfolio_flag",
            F.col("total_principal") > F.col("high_principal")
        )
        .select(
            "segment_name",
            "total_principal",
            "high_portfolio_flag"
        )
    )
    return flagged

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
