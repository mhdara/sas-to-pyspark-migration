import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("10_repayment_forecast").getOrCreate()

# Read input data
loan_payments_df = spark.read.parquet("data/parquet/stg/loan_payments.parquet")

# Create out.monthly_repay
monthly_repay_df = (
    loan_payments_df
    .withColumn("month", F.trunc("payment_date", "month"))
    .groupBy("month")
    .agg(F.sum("payment_amount").alias("total_payment"))
    .orderBy("month")
)

# Write out.monthly_repay
monthly_repay_df.write.mode("overwrite").parquet("data/parquet/py_out/llm_local/run1/monthly_repay")

# Forecast using brown_expo
from forecast import brown_expo

# Prepare data for forecasting
forecast_data = (
    monthly_repay_df
    .select("month", "total_payment")
    .orderBy("month")
    .collect()
)

# Extract the payment amounts as a list
payment_amounts = [row["total_payment"] for row in forecast_data]

# Perform forecasting
fitted, future, states = brown_expo(payment_amounts, weight=0.2, nstart=12, lead=12)

# Create out.fc
fc_data = []
for i, (month, payment) in enumerate(forecast_data):
    fc_data.append({"month": month, "_type_": "ACTUAL", "_lead_": 0, "total_payment": float(payment)})
    if i < len(fitted):
        fc_data.append({"month": month, "_type_": "FORECAST", "_lead_": 0, "total_payment": float(fitted[i])})
for i, payment in enumerate(future):
    fc_data.append({"month": forecast_data[-1]["month"] + F.expr(f"add_months(month, {i+1})"), "_type_": "FORECAST", "_lead_": i+1, "total_payment": float(payment)})

fc_df = spark.createDataFrame(fc_data)

# Write out.fc
fc_df.write.mode("overwrite").parquet("data/parquet/py_out/llm_local/run1/fc")

# Create out.fc_est
fc_est_data = [
    {"_type_": "S1", "month": forecast_data[-1]["month"], "total_payment": float(states["S1"])},
    {"_type_": "S2", "month": forecast_data[-1]["month"], "total_payment": float(states["S2"])},
    {"_type_": "CONSTANT", "month": forecast_data[-1]["month"], "total_payment": float(states["CONSTANT"])},
    {"_type_": "LINEAR", "month": forecast_data[-1]["month"], "total_payment": float(states["LINEAR"])}
]

fc_est_df = spark.createDataFrame(fc_est_data)

# Write out.fc_est
fc_est_df.write.mode("overwrite").parquet("data/parquet/py_out/llm_local/run1/fc_est")
