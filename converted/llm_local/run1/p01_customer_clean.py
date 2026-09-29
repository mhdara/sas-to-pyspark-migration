import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("01_customer_clean").getOrCreate()

# Read the source table
df = spark.read.parquet("data/parquet/stg/customers.parquet")

# Remove leading/trailing blanks from the name, upper-case the province, flag missing income
df = df.withColumn("customer_name", F.trim(F.col("customer_name")))
df = df.withColumn("province", F.upper(F.col("province")))
df = df.withColumn("income_missing", F.col("annual_income").isNull().cast("int"))

# Assign income band
df = df.withColumn("income_band", 
    F.when(F.col("annual_income").isNull() | (F.col("annual_income") < 20000), 'LOW')
     .when(F.col("annual_income") < 80000, 'MID')
     .otherwise('HIGH')
)

# Write the output table
df.write.mode("overwrite").parquet("data/parquet/py_out/llm_local/run1/customers_clean")
