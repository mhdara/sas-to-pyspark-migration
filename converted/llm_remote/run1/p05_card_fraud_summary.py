import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("05_card_fraud_summary").getOrCreate()

STG = "data/parquet/stg"
OUT = "data/parquet/py_out/llm_remote/run1"

card_transactions = spark.read.parquet(f"{STG}/card_transactions.parquet")

# ---------------------------------------------------------------------------
# PROC MEANS data=stg.card_transactions noprint nway;
#   class merchant_category; var amount;
#   output out=out.card_summary(drop=_type_ _freq_)
#          n=n_txn sum=total_amount mean=avg_amount;
# SAS excludes rows with a missing CLASS value; N counts NON-MISSING amounts.
# Negative (refund) and zero amounts are kept, they lower sum and mean.
# ---------------------------------------------------------------------------
card_summary = (
    card_transactions
    .filter(F.col("merchant_category").isNotNull())
    .groupBy("merchant_category")
    .agg(
        F.count(F.col("amount")).alias("n_txn"),
        F.sum(F.col("amount")).alias("total_amount"),
        F.avg(F.col("amount")).alias("avg_amount"),
    )
    .orderBy("merchant_category")
)

card_summary.write.mode("overwrite").parquet(f"{OUT}/card_summary")

# ---------------------------------------------------------------------------
# PROC FREQ data=stg.card_transactions noprint;
#   tables merchant_category*fraud_flag / out=out.fraud_freq;
# Rows with a missing merchant_category or fraud_flag are excluded (SAS default),
# percent = 100 * count / total of the non-missing rows.
# ---------------------------------------------------------------------------
freq_base = card_transactions.filter(
    F.col("merchant_category").isNotNull() & F.col("fraud_flag").isNotNull()
)

total_rows = freq_base.count()

fraud_freq = (
    freq_base
    .groupBy("merchant_category", "fraud_flag")
    .agg(F.count(F.lit(1)).alias("count"))
    .withColumn(
        "percent",
        F.when(F.lit(total_rows) == 0, F.lit(None).cast("double"))
         .otherwise(F.lit(100.0) * F.col("count") / F.lit(total_rows)),
    )
    .select(
        F.col("merchant_category"),
        F.col("fraud_flag").cast("int").alias("fraud_flag"),
        F.col("count").cast("long").alias("count"),
        F.col("percent").cast("double").alias("percent"),
    )
    .orderBy("merchant_category", "fraud_flag")
)

fraud_freq.write.mode("overwrite").parquet(f"{OUT}/fraud_freq")

spark.stop()
