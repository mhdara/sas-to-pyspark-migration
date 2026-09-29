import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("01_customer_clean").getOrCreate()

STG_PATH = "data/parquet/stg"
OUT_PATH = "data/parquet/py_out/llm_remote/run1"

# set stg.customers
customers = spark.read.parquet(f"{STG_PATH}/customers.parquet")

df = customers

# customer_name = strip(customer_name)
df = df.withColumn("customer_name", F.trim(F.col("customer_name")))

# province = upcase(province)
df = df.withColumn("province", F.upper(F.col("province")))

# income_missing = missing(annual_income)  -> integer flag 0/1
df = df.withColumn("income_missing", F.col("annual_income").isNull().cast("int"))

# TRAP (R3): in SAS a missing number is SMALLER than any number,
# so "annual_income < 20000" is TRUE when annual_income is missing -> LOW
df = df.withColumn(
    "income_band",
    F.when(F.col("annual_income").isNull() | (F.col("annual_income") < 20000), F.lit("LOW"))
     .when(F.col("annual_income") < 80000, F.lit("MID"))
     .otherwise(F.lit("HIGH"))
)

df.write.mode("overwrite").parquet(f"{OUT_PATH}/customers_clean")

spark.stop()
