import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("08_period_driver").getOrCreate()

# Read the active periods from the reporting_periods table
active_periods_df = spark.read.parquet("data/parquet/ctrl/reporting_periods.parquet")
active_periods = active_periods_df.filter(F.col("active_flag") == 1).orderBy("period_id").select("period_id").collect()
period_list = [row["period_id"] for row in active_periods]

# Include the macro library
from p06_macro_library import summarize

def run_period(pid):
    # Filter the delinquency data for the given period
    pay_df = spark.read.parquet("data/parquet/out/delinquency").filter(F.col("period_id") == pid)
    # Call the summarize function
    summarize(ds=pay_df, class_col="delinquent_flag", var="payment_amount", out=f"data/parquet/out/period_summary_{pid}")

def period_driver():
    for pid in period_list:
        run_period(pid)

period_driver()
