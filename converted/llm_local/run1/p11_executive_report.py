import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("11_executive_report").getOrCreate()

# Read the DPD_THRESHOLD from the settings table
dpd_threshold_df = spark.read.parquet("data/parquet/ctrl/macro_parameters.parquet")
dpd_threshold = dpd_threshold_df.filter(F.col("param_name") == 'DPD_THRESHOLD').select("param_value").first()[0]

# Create the exec_report DataFrame
loan_enriched_df = spark.read.parquet("data/parquet/py_out/llm_local/run1/loan_enriched")
loan_status_df = spark.read.parquet("data/parquet/py_out/llm_local/run1/loan_status")

exec_report_df = (loan_enriched_df
    .alias("e")
    .join(loan_status_df.alias("s"), F.col("e.loan_id") == F.col("s.loan_id"), "left")
    .groupBy("e.segment_name")
    .agg(
        F.countDistinct("e.loan_id").alias("n_loans"),
        F.sum(F.when(F.col("s.max_dpd") > dpd_threshold, 1).otherwise(0)).alias("n_delinquent_loans"),
        F.sum("e.principal").alias("total_principal")
    ))

# Write the exec_report DataFrame to the output path
exec_report_df.write.mode("overwrite").parquet("data/parquet/py_out/llm_local/run1/exec_report")

# Create the exec_forecast DataFrame
fc_df = spark.read.parquet("data/parquet/py_out/llm_local/run1/fc")
monthly_repay_df = spark.read.parquet("data/parquet/py_out/llm_local/run1/monthly_repay")

max_month = monthly_repay_df.select(F.max("month")).first()[0]

exec_forecast_df = (fc_df
    .filter((F.col("_type_") == 'FORECAST') & (F.col("month") > max_month))
    .select("month", "total_payment")
    .withColumnRenamed("total_payment", "forecast_payment"))

# Write the exec_forecast DataFrame to the output path
exec_forecast_df.write.mode("overwrite").parquet("data/parquet/py_out/llm_local/run1/exec_forecast")
