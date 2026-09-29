import os, sys
import datetime
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, DoubleType

from forecast import brown_expo

spark = SparkSession.builder.master("local[*]").appName("10_repayment_forecast").getOrCreate()

STG = "data/parquet/stg"
OUT = "data/parquet/py_out/llm_remote/run1"

# ------------------------------------------------------------------
# proc sql: total payments per calendar month
#   intnx('month', payment_date, 0, 'b')  ->  F.trunc(payment_date, "month")
# ------------------------------------------------------------------
loan_payments = spark.read.parquet(f"{STG}/loan_payments.parquet")

monthly_repay = (
    loan_payments
    .withColumn("month", F.trunc(F.col("payment_date"), "month"))
    .groupBy("month")
    .agg(F.sum("payment_amount").alias("total_payment"))
    .orderBy("month")
)

monthly_repay.write.mode("overwrite").parquet(f"{OUT}/monthly_repay")

# re-read what was written, so the forecast uses exactly the stored table (out.monthly_repay)
monthly_repay = spark.read.parquet(f"{OUT}/monthly_repay").orderBy("month")

# ------------------------------------------------------------------
# proc forecast data=out.monthly_repay interval=month
#               method=expo trend=2 weight=0.2 nstart=12 lead=12
#               out=out.fc outfull outest=out.fc_est
#   id month; var total_payment;
# PROC FORECAST does not exist in Python: the method is rebuilt by hand.
# ------------------------------------------------------------------
hist_rows = monthly_repay.select("month", "total_payment").collect()
months = [r["month"] for r in hist_rows]
y = [float(r["total_payment"]) for r in hist_rows]

WEIGHT = 0.2
NSTART = 12
LEAD = 12

fitted, future, states = brown_expo(y, WEIGHT, NSTART, LEAD)


def add_months(d, n):
    m0 = d.year * 12 + (d.month - 1) + n
    return datetime.date(m0 // 12, m0 % 12 + 1, 1)


rows_fc = []
# history: ACTUAL rows and FORECAST (fitted) rows, _lead_ = 0
for i, m in enumerate(months):
    rows_fc.append((m, "ACTUAL", 0, float(y[i])))
    fv = fitted[i]
    rows_fc.append((m, "FORECAST", 0, None if fv is None else float(fv)))

# future: FORECAST rows, _lead_ = 1..LEAD
last_month = months[-1]
for k in range(1, LEAD + 1):
    rows_fc.append((add_months(last_month, k), "FORECAST", k, float(future[k - 1])))

# The 95% confidence limits (L95/U95 rows) and fit statistics are not reproduced.  # TODO MANUAL

schema_fc = StructType([
    StructField("month", DateType(), True),
    StructField("_type_", StringType(), True),
    StructField("_lead_", IntegerType(), True),
    StructField("total_payment", DoubleType(), True),
])

fc = spark.createDataFrame(rows_fc, schema=schema_fc).orderBy("month", "_lead_", "_type_")
fc.write.mode("overwrite").parquet(f"{OUT}/fc")

# outest: one row per final state, month = last history month
rows_est = []
for t in ["S1", "S2", "CONSTANT", "LINEAR"]:
    v = states.get(t)
    rows_est.append((t, last_month, None if v is None else float(v)))

schema_est = StructType([
    StructField("_type_", StringType(), True),
    StructField("month", DateType(), True),
    StructField("total_payment", DoubleType(), True),
])

fc_est = spark.createDataFrame(rows_est, schema=schema_est)
fc_est.write.mode("overwrite").parquet(f"{OUT}/fc_est")

spark.stop()
