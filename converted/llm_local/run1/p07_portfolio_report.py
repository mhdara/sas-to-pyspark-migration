import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("07_portfolio_report").getOrCreate()

# Read the HIGH_PRINCIPAL threshold from the settings table
high_principal = spark.read.parquet("data/parquet/ctrl/macro_parameters.parquet") \
    .filter(F.col("param_name") == 'HIGH_PRINCIPAL') \
    .select("param_value") \
    .first()[0]

# Include the macro library
from p06_macro_library import summarize, flag_high

# Call the summarize macro to total the loan principal per segment
summarize(ds="data/parquet/py_out/llm_local/run1/loan_enriched", class="segment_name", var="principal", out="data/parquet/py_out/llm_local/run1/portfolio_by_segment")

# Call the flag_high macro to flag segments whose total is above the threshold
flag_high(ds="data/parquet/py_out/llm_local/run1/portfolio_by_segment", var="total", threshold=high_principal, out="data/parquet/py_out/llm_local/run1/portfolio_report")
