import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("03_risk_format").getOrCreate()

CTRL = "data/parquet/ctrl"
STG = "data/parquet/stg"
OUT = "data/parquet/py_out/llm_remote/run1"

# Reads: out.customers_clean (written by program 01)
customers_clean = spark.read.parquet(f"{OUT}/customers_clean")

# PROC FORMAT value incband -> rebuilt as an F.when chain in the same order.
#   low -< 20000   = 'LOW'      (strictly less than 20000; LOW does NOT include missing)
#   20000 -< 80000 = 'MID'
#   80000 - high   = 'HIGH'     (>= 80000)
#   other          = 'UNKNOWN'  (OTHER also catches missing values)
income = F.col("annual_income")
income_band_fmt = (
    F.when(income.isNull(), F.lit("UNKNOWN"))          # missing -> OTHER -> 'UNKNOWN'
     .when(income < F.lit(20000), F.lit("LOW"))
     .when(income < F.lit(80000), F.lit("MID"))
     .when(income >= F.lit(80000), F.lit("HIGH"))
     .otherwise(F.lit("UNKNOWN"))
)

# length income_band_fmt $7 -> truncate to 7 characters to match SAS storage length
customer_risk = customers_clean.withColumn(
    "income_band_fmt", F.substring(income_band_fmt.cast("string"), 1, 7)
)

customer_risk.write.mode("overwrite").parquet(f"{OUT}/customer_risk")

spark.stop()
