import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("03_risk_format").getOrCreate()

# Read the input data
df = spark.read.parquet("data/parquet/py_out/llm_local/run1/customers_clean.parquet")

# Define the income band format using F.when
df = df.withColumn("income_band_fmt", 
    F.when(F.col("annual_income").isNull(), "UNKNOWN")
    .when(F.col("annual_income") < 20000, "LOW")
    .when(F.col("annual_income") < 80000, "MID")
    .otherwise("HIGH")
)

# Write the output data
df.write.mode("overwrite").parquet("data/parquet/py_out/llm_local/run1/customer_risk")
