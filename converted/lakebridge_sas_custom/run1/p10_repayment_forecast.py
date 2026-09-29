# Databricks notebook source
# MAGIC %md
# MAGIC # 10_repayment_forecast
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260929024630-suw5/10_repayment_forecast.sas`
# COMMAND ----------
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql import Window
import pandas as pd

# COMMAND ----------
# ------------------------------------------------------------------
# Step 1: Aggregate loan payments to monthly totals
# ------------------------------------------------------------------
# Read the source table (Unity Catalog schema workspace.stg)
df_payments = spark.read.table("workspace.stg.loan_payments")

# COMMAND ----------
# Create a column that is the first day of the payment month (intnx with 'b')
df_monthly = (
    df_payments
    .withColumn("month", F.trunc(F.col("payment_date"), "month"))
    .groupBy("month")
    .agg(F.sum(F.col("payment_amount")).alias("total_payment"))
    .orderBy("month")
)

# COMMAND ----------
# Write the monthly aggregation (48 rows expected)
df_monthly.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.out.monthly_repay")

# COMMAND ----------
# ------------------------------------------------------------------
# Step 2: Double exponential smoothing forecast (PROC FORECAST)
# ------------------------------------------------------------------
# PROC FORECAST is not available in Spark.  The SAS options are reproduced
# by calling the custom implementation `brown_expo` (see module forecast.py).
# The implementation follows the SAS specification:
#   METHOD=EXPO  TREND=2  WEIGHT=0.2  NSTART=12  LEAD=12
# The function returns:
#   fitted  – fitted values for the historical periods
#   future  – forecasts for the lead periods
#   states  – a dict with the final internal state values (S1, S2, CONSTANT, LINEAR)

# -------------------------------------------------
# 2.1 Pull the data into pandas for the forecast
# -------------------------------------------------
pdf = df_monthly.select("month", "total_payment") \
    .orderBy("month") \
    .toPandas()

# COMMAND ----------
# Ensure the series is numeric and sorted by month
y = pdf["total_payment"].astype(float).values
months = pd.to_datetime(pdf["month"]).to_series().reset_index(drop=True)

# COMMAND ----------
# -------------------------------------------------
# 2.2 Run the forecast
# -------------------------------------------------
from forecast import brown_expo  # custom module placed in the same folder

# COMMAND ----------
weight = 0.2
nstart = 12
lead   = 12

# COMMAND ----------
fitted, future, states = brown_expo(y, weight=weight, nstart=nstart, lead=lead)

# COMMAND ----------
# -------------------------------------------------
# 2.3 Build the output table `out.fc`
# -------------------------------------------------
#  * ACTUAL rows – the original payments
#  * FORECAST rows – fitted values (lead = 0) and future forecasts (lead = 1‑lead)
actual_df = pd.DataFrame({
    "month": months,
    "_type_": "ACTUAL",
    "_lead_": 0,
    "total_payment": y
})

# COMMAND ----------
forecast_hist_df = pd.DataFrame({
    "month": months,
    "_type_": "FORECAST",
    "_lead_": 0,
    "total_payment": fitted
})

# COMMAND ----------
# Generate future months (first day of each month after the last observed month)
last_month = months.iloc[-1]
future_months = pd.date_range(start=last_month + pd.offsets.MonthBegin(1),
                              periods=lead,
                              freq="MS")

# COMMAND ----------
forecast_future_df = pd.DataFrame({
    "month": future_months,
    "_type_": "FORECAST",
    "_lead_": list(range(1, lead + 1)),
    "total_payment": future
})

# COMMAND ----------
# Concatenate all parts
df_fc_pd = pd.concat([actual_df, forecast_hist_df, forecast_future_df],
                     ignore_index=True)

# COMMAND ----------
# Convert to a Spark DataFrame (Spark will infer the correct types)
df_fc = spark.createDataFrame(df_fc_pd)

# COMMAND ----------
# Write the forecast table (includes ACTUAL, fitted, and future rows)
df_fc.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.out.fc")

# COMMAND ----------
# -------------------------------------------------
# 2.4 Build the output table `out.fc_est`
# -------------------------------------------------
# The EST table contains one row per internal state returned by the
# forecasting routine.  Each row shows the state value for the last
# observed month.
est_rows = [
    {"_type_": state_name,
     "month": last_month,
     "total_payment": float(state_value)}
    for state_name, state_value in states.items()
]

# COMMAND ----------
df_fc_est = spark.createDataFrame(pd.DataFrame(est_rows))

# COMMAND ----------
df_fc_est.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.out.fc_est")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
