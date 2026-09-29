# Databricks notebook source
# MAGIC %md
# MAGIC # 01_customer_clean
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928145005-uq1l/01_customer_clean.sas`
# COMMAND ----------
# COMMAND ----------
# ------------------------------------------------------------
# 01_customer_clean.py  –  Databricks (PySpark) conversion
# ------------------------------------------------------------

# 1.  Imports & window functions
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------
# 2.  Parameters (maps SAS macro variables)
# In SAS:  libname stg "&root/stg";  libname out "&root/out";
# In Databricks we expose the same `root` path as a widget.
#   Example usage in a notebook cell before running this code:
#   dbutils.widgets.text("root", "/mnt/data")
root_path = dbutils.widgets.get("root")          # e.g. "/mnt/data"

# COMMAND ----------
# 3.  Read the source table (equivalent to `set stg.customers;`)
# Assuming the staging data is registered as a Delta table in the Unity Catalog:
stg_customers_tbl = f"{root_path}/stg.customers"   # path or catalog.schema.table
df_customers = spark.read.table("stg.customers")   # replace with the actual table name

# COMMAND ----------
# 4.  Transformations ------------------------------------------------
# - Trim blanks from `customer_name`
# - Upper‑case `province`
# - Detect missing income (null in Spark)
# - Assign income band with the SAS‑specific missing‑value trap
df_clean = (
    df_customers
    # remove leading/trailing blanks
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    # uppercase province
    .withColumn("province", F.upper(F.col("province")))
    # flag missing income (optional, kept for parity with SAS)
    .withColumn("income_missing", F.col("annual_income").isNull())
    # income band – treat NULL as a value smaller than any number
    .withColumn(
        "income_band",
        F.when(
            (F.col("annual_income").isNull()) | (F.col("annual_income") < 20000),
            F.lit("LOW")
        )
        .when(F.col("annual_income") < 80000, F.lit("MID"))
        .otherwise(F.lit("HIGH"))
    )
    # keep only the columns needed for the output (optional)
    .select(
        "customer_id",          # assuming an ID column exists
        "customer_name",
        "province",
        "annual_income",
        "income_missing",
        "income_band"
    )
)

# COMMAND ----------
# 5.  Write the cleaned data (equivalent to `data out.customers_clean; ... run;`)
# The target is a Delta table under the `out` schema. Adjust the write mode as required.
output_table = "out.customers_clean"   # replace with full catalog/schema.table if needed
(
    df_clean
    .write
    .mode("overwrite")                # overwrites if the table already exists
    .saveAsTable(output_table)
)

# COMMAND ----------
# 6.  Quick preview (Databricks visualisation)
display(df_clean)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
