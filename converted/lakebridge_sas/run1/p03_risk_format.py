# Databricks notebook source
# MAGIC %md
# MAGIC # 03_risk_format
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928142931-up2j/03_risk_format.sas`
# COMMAND ----------
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

# COMMAND ----------
# ----------------------------------------------------------------------
# READ SOURCE TABLE
# ----------------------------------------------------------------------
# Assuming the SAS library reference `out` maps to a Unity Catalog schema
# called `out` (e.g., catalog = "my_catalog", schema = "out").
# Adjust the fully‑qualified name as needed for your environment.
df_customers = spark.read.table("my_catalog.out.customers_clean")

# COMMAND ----------
# ----------------------------------------------------------------------
# SAS FORMAT `incband.` TO PYTHON
# ----------------------------------------------------------------------
# The SAS format `incband.` (e.g., 0-19999 = 'Low', 20000-99999 = 'Med', etc.)
# must be recreated in Python. Below is a placeholder dictionary; replace
# it with the actual band definitions from your SAS format library.
incband_map = {
    (0,      19999): "Low",
    (20000,  99999): "Med",
    (100000, 999999): "High",
    # Add additional bands as required
}

# COMMAND ----------
def map_income_band(income):
    """Return the income band label for a given annual_income."""
    if income is None:
        return None
    for (low, high), label in incband_map.items():
        if low <= income <= high:
            return label
    return "Other"

# COMMAND ----------
# Register the mapping function as a Spark UDF (returns a string of length ≤7)
income_band_udf = F.udf(map_income_band, StringType())

# COMMAND ----------
# ----------------------------------------------------------------------
# CREATE TARGET DATAFRAME (equivalent to SAS DATA step)
# ----------------------------------------------------------------------
df_customer_risk = (
    df_customers
    # Apply the income band format; column length constraint ($7) is handled
    # automatically by Spark (StringType) – you can truncate if necessary.
    .withColumn("income_band_fmt", income_band_udf(F.col("annual_income")))
)

# COMMAND ----------
# ----------------------------------------------------------------------
# WRITE RESULT (optional)
# ----------------------------------------------------------------------
# If you need to persist the result as a Delta table:
# df_customer_risk.write.mode("overwrite").saveAsTable("my_catalog.out.customer_risk")

# For interactive inspection in a Databricks notebook:
display(df_customer_risk)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
