import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("05_card_fraud_summary").getOrCreate()

# Read the source table
df = spark.read.parquet("data/parquet/stg/card_transactions.parquet")

# PROC MEANS equivalent
card_summary = df.groupBy("merchant_category").agg(
    F.count("amount").alias("n_txn"),
    F.sum("amount").alias("total_amount"),
    F.mean("amount").alias("avg_amount")
)

# Write the output table
card_summary.write.mode("overwrite").parquet("data/parquet/py_out/llm_local/run1/card_summary")

# PROC FREQ equivalent
fraud_freq = df.groupBy("merchant_category", "fraud_flag").count()
total_count = df.count()
fraud_freq = fraud_freq.withColumn("percent", (F.col("count") / total_count) * 100)

# Write the output table
fraud_freq.write.mode("overwrite").parquet("data/parquet/py_out/llm_local/run1/fraud_freq")
