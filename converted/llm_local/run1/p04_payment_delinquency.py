import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("04_payment_delinquency").getOrCreate()

# Read input tables
macro_parameters = spark.read.parquet("data/parquet/ctrl/macro_parameters.parquet")
loan_payments = spark.read.parquet("data/parquet/stg/loan_payments.parquet")

# Read the late-payment threshold from the settings table
dpd_threshold = macro_parameters.filter(F.col("param_name") == "DPD_THRESHOLD").select("param_value").first()[0]

# Sort the payments by loan and date and remove duplicates
pay_sorted = loan_payments.orderBy("loan_id", "payment_date").distinct()

# Define window specification for running total and max
w = Window.partitionBy("loan_id").orderBy("payment_date").rowsBetween(Window.unboundedPreceding, 0)

# Calculate running total paid, highest days past due, late flag, and payment month
delinquency = pay_sorted.withColumn("cum_paid", F.sum(F.coalesce(F.col("payment_amount"), F.lit(0))).over(w)) \
                       .withColumn("max_dpd", F.max(F.col("days_past_due")).over(w)) \
                       .withColumn("delinquent_flag", F.when(F.col("days_past_due") > int(dpd_threshold), 1).otherwise(0)) \
                       .withColumn("period_id", F.date_format(F.col("payment_date"), "yyyyMM"))

# Write the delinquency table
delinquency.write.mode("overwrite").parquet("data/parquet/py_out/llm_local/run1/delinquency")

# Calculate summary for each loan
loan_status = delinquency.withColumn("row_num", F.row_number().over(Window.partitionBy("loan_id").orderBy(F.desc("payment_date")))) \
                         .filter(F.col("row_num") == 1) \
                         .select("loan_id", "cum_paid", "max_dpd")

# Write the loan status table
loan_status.write.mode("overwrite").parquet("data/parquet/py_out/llm_local/run1/loan_status")
