import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("02_loan_enrichment").getOrCreate()

CTRL = "data/parquet/ctrl"
STG = "data/parquet/stg"
OUT = "data/parquet/py_out/llm_remote/run1"

# stg.loans  -> raw parquet from the SAS staging library
loans = spark.read.parquet(f"{STG}/loans.parquet").alias("l")
# out.customers_clean -> written by converted program 01
customers = spark.read.parquet(f"{OUT}/customers_clean").alias("c")
# ctrl.client_segments -> control/lookup parquet
segments = spark.read.parquet(f"{CTRL}/client_segments.parquet").alias("s")

# proc sql:
#   select l.*, c.customer_name, c.province, c.income_band, s.segment_name, s.risk_band
#   from stg.loans l
#        inner join out.customers_clean c on l.customer_id = c.customer_id
#        left  join ctrl.client_segments s on c.segment_code = s.segment_code
joined = (
    loans
    .join(customers, F.col("l.customer_id") == F.col("c.customer_id"), "inner")
    .join(segments, F.col("c.segment_code") == F.col("s.segment_code"), "left")
)

select_cols = [F.col("l." + c).alias(c) for c in loans.columns] + [
    F.col("c.customer_name").alias("customer_name"),
    F.col("c.province").alias("province"),
    F.col("c.income_band").alias("income_band"),
    F.col("s.segment_name").alias("segment_name"),
    F.col("s.risk_band").alias("risk_band"),
]

loan_enriched = joined.select(*select_cols)

loan_enriched.write.mode("overwrite").parquet(f"{OUT}/loan_enriched")

spark.stop()
