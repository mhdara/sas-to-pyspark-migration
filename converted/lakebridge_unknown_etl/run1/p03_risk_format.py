# Databricks notebook source
# MAGIC %md
# MAGIC # 03_risk_format
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928141958-i75y/03_risk_format.sas`
# COMMAND ----------
# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------
# -------------------------------------------------------------------------
# Helper: replicate the SAS format "incband."
# The exact mapping depends on the original SAS format definition.
# Below is a placeholder implementation that can be customized.
# -------------------------------------------------------------------------
def map_income_to_band(income):
    """
    Map a numeric annual_income to a formatted income band string.
    This mimics the SAS format `incband.` used in the original SAS step.
    Adjust the thresholds and labels to match the real SAS format.
    """
    if income is None:
        return None
    # Example band definitions – replace with actual logic from incband.
    if income < 25000:
        return "LOW"
    elif income < 50000:
        return "MEDIUM"
    elif income < 100000:
        return "HIGH"
    else:
        return "VERYHIGH"

# COMMAND ----------
# Register the function as a Spark UDF so it can be used in a DataFrame expression.
income_band_udf = F.udf(map_income_to_band, StringType())

# COMMAND ----------
def customer_risk():
    # Read the source dataset produced by the previous SAS step (out.customers_clean)
    # In a DLT pipeline, the source is referenced by its table name without the
    # schema prefix; adjust the name if your source lives in a different database.
    source_df = dlt.read("customers_clean")

    # Apply the income band transformation and enforce a max length of 7 characters
    # (matching the SAS LENGTH statement). Truncate if necessary.
    result_df = (
        source_df
        .withColumn(
            "income_band_fmt",
            F.substring(income_band_udf(F.col("annual_income")), 1, 7)
        )
    )

    return result_df
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
