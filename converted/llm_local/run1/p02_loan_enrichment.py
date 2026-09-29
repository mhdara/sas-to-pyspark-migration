import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("02_loan_enrichment").getOrCreate()

# Read input tables
loans_df = spark.read.parquet("data/parquet/stg/loans.parquet")
customers_clean_df = spark.read.parquet("data/parquet/py_out/llm_local/run1/customers_clean")
client_segments_df = spark.read.parquet("data/parquet/ctrl/client_segments.parquet")

# Perform the joins
loan_enriched_df = loans_df.join(customers_clean_df, on="customer_id", how="inner") \
                           .join(client_segments_df, on="segment_code", how="left") \
                           .select(loans_df["*"], 
                                   customers_clean_df["customer_name"], 
                                   customers_clean_df["province"], 
                                   customers_clean_df["income_band"], 
                                   client_segments_df["segment_name"], 
                                   client_segments_df["risk_band"])

# Write the output table
loan_enriched_df.write.mode("overwrite").parquet("data/parquet/py_out/llm_local/run1/loan_enriched")
