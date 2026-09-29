# Databricks notebook source
# MAGIC %md
# MAGIC # 04_payment_delinquency
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928141958-i75y/04_payment_delinquency.sas`
# COMMAND ----------
# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# ----------------------------------------------------------------------
# Parameter definition: threshold for days past due (DPD) that marks a loan as delinquent.
# This can be supplied at pipeline run time; defaults to 30 days if not provided.
# ----------------------------------------------------------------------
dpd_threshold = dlt.Parameter("dpd_threshold", default=30)

# COMMAND ----------
def raw_payments():
    # Example: reading from a Delta table or external source.
    # Adjust the format and options as needed for your environment.
    return (
        spark.readStream.format("delta")
        .table("source.payments")
    )

# COMMAND ----------
def delinquency():
    # Window that looks at all rows for a loan up to the current row
    loan_window = Window.partitionBy("loan_id").orderBy(F.col("payment_date")).rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )

    return (
        dlt.read_stream("raw_payments")
        # Derive period_id in the format YYYYMM (e.g., 202309)
        .withColumn("period_id", F.date_format(F.col("payment_date"), "yyyyMM"))
        # Cumulative maximum DPD per loan
        .withColumn("max_dpd", F.max("days_past_due").over(loan_window))
        # Flag if current DPD exceeds the threshold
        .withColumn(
            "delinquent_flag",
            F.col("days_past_due") > dpd_threshold
        )
        # Keep only the columns required downstream (adjust as needed)
        .select(
            "loan_id",
            "payment_date",
            "days_past_due",
            "period_id",
            "max_dpd",
            "delinquent_flag"
        )
    )

# COMMAND ----------
def loan_status():
    # Read the enriched delinquency view (can be a streaming or batch read)
    df = dlt.read("delinquency")

    # Aggregate per loan_id
    agg_expr = [
        F.max("max_dpd").alias("max_dpd"),
        # Any row where delinquent_flag is true sets the loan as delinquent
        F.max(F.when(F.col("delinquent_flag"), F.lit(1)).otherwise(F.lit(0))).cast("boolean").alias("delinquent_flag")
    ]

    return (
        df.groupBy("loan_id")
          .agg(*agg_expr)
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
