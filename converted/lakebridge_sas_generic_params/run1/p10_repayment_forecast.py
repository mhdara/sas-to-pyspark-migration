# Databricks notebook source
# MAGIC %md
# MAGIC # 10_repayment_forecast
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928145005-uq1l/10_repayment_forecast.sas`
# COMMAND ----------
# COMMAND ----------
# ------------------------------------------------------------
# 10_repayment_forecast.py   (Databricks‑optimized PySpark)
# ------------------------------------------------------------
# Objective:
#   1. Summarise loan payments by calendar month (48 months: 2022‑2025).
#   2. Produce a 12‑month double‑exponential‑smoothing forecast
#      (equivalent to PROC FORECAST with METHOD=EXPO TREND=2,
#       WEIGHT=0.2, NSTART=12, LEAD=12).
# ------------------------------------------------------------

# ------------------------------------------------------------
# 0) Environment & helper imports
# ------------------------------------------------------------
import pyspark.sql.functions as F
from pyspark.sql import Window
from datetime import datetime
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# COMMAND ----------
# ------------------------------------------------------------
# 1) Parameters (SAS macro variables become Databricks widgets)
# ------------------------------------------------------------
# In the original SAS code: libname stg "&root/stg";  libname out "&root/out";
# Here we expect a widget named "root" that points to the base catalog/schema.
# Example usage in a notebook before running this script:
#   dbutils.widgets.text("root", "my_catalog.my_schema")
root = dbutils.widgets.get("root")          # e.g. "my_catalog.my_schema"
stg_path = f"{root}.stg"                    # schema for source tables
out_path = f"{root}.out"                    # schema for destination tables

# COMMAND ----------
# ------------------------------------------------------------
# 2) Read source data (stg.loan_payments)
# ------------------------------------------------------------
df_payments = spark.read.table(f"{stg_path}.loan_payments")

# COMMAND ----------
# ------------------------------------------------------------
# 3) PROC SQL equivalent – monthly aggregation
# ------------------------------------------------------------
# SAS: intnx('month', payment_date, 0, 'b')  --> first day of month (beginning)
# In Spark we can use date_trunc('month', …) and cast to date.
df_monthly = (
    df_payments
    .withColumn(
        "month",
        F.to_date(F.date_trunc("month", F.col("payment_date")))  # start of month
    )
    .groupBy("month")
    .agg(F.sum(F.col("payment_amount")).alias("total_payment"))
    .orderBy("month")
)

# COMMAND ----------
# Write the result to the output schema (equivalent to PROC SQL CREATE TABLE)
df_monthly.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{out_path}.monthly_repay")

# COMMAND ----------
# ------------------------------------------------------------
# 4) PROC FORECAST equivalent – double exponential smoothing
# ------------------------------------------------------------
# Spark does not have a built‑in forecasting procedure.
# We collect the monthly series to pandas, fit a Holt’s linear trend model
# (double exponential smoothing) with the same parameters that SAS used,
# and then push the results back to Spark.

# 4.1) Pull the aggregated data into pandas (48 rows – small enough)
pdf = df_monthly.select("month", "total_payment") \
                .orderBy("month") \
                .toPandas()

# COMMAND ----------
# Ensure the index is a proper datetime series (required by statsmodels)
pdf["month"] = pd.to_datetime(pdf["month"])
pdf.set_index("month", inplace=True)

# COMMAND ----------
# 4.2) Fit the model
#   - method=expo trend=2  → Holt’s linear trend (additive)
#   - weight=0.2           → smoothing_level = 0.2 (level) and smoothing_slope = 0.2 (trend)
#   - nstart=12           → the first 12 observations are used for initialization;
#                           statsmodels does this internally when initialization_method='estimated'.
model = ExponentialSmoothing(
    pdf["total_payment"],
    trend="add",
    damped_trend=False,
    initialization_method="estimated"
).fit(
    smoothing_level=0.2,      # corresponds to WEIGHT=0.2 for the level component
    smoothing_slope=0.2,      # corresponds to WEIGHT=0.2 for the trend component
    optimized=False           # keep user‑specified weights
)

# COMMAND ----------
# 4.3) Create output DataFrames
#    - Actuals (original 48 months)
#    - Fitted values (in‑sample predictions)
#    - Forecasts for the next 12 months (lead=12)

# In‑sample fitted values
fitted = model.fittedvalues
fitted_df = fitted.reset_index()
fitted_df.columns = ["month", "fitted_total_payment"]

# COMMAND ----------
# Forecast for next 12 months
forecast = model.forecast(12)
forecast_df = forecast.reset_index()
forecast_df.columns = ["month", "forecast_total_payment"]

# COMMAND ----------
# Combine actuals, fitted, and forecasts into one DataFrame (mirrors OUTFULL)
full_df = (
    pd.concat([pdf.reset_index(), fitted_df, forecast_df], ignore_index=True)
    .sort_values("month")
)

# COMMAND ----------
# ------------------------------------------------------------
# 5) Write forecast results back to Delta tables
# ------------------------------------------------------------
# 5.1) out.fc  – contains actuals, fitted values, and forecasts
df_fc = spark.createDataFrame(full_df)
df_fc.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{out_path}.fc")

# COMMAND ----------
# 5.2) out.fc_est – internal model parameters (level, trend, etc.)
# Statsmodels stores these in model.params
est_params = pd.DataFrame({
    "parameter": ["level", "trend"],
    "value": [model.params["initial_level"], model.params["initial_trend"]]
})
df_fc_est = spark.createDataFrame(est_params)
df_fc_est.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{out_path}.fc_est")

# COMMAND ----------
# ------------------------------------------------------------
# 6) Optional: display results in a Databricks notebook
# ------------------------------------------------------------
display(df_monthly)   # 48‑month actual totals
display(df_fc)        # actuals + fitted + 12‑month forecast
display(df_fc_est)    # model‐internal estimates

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
