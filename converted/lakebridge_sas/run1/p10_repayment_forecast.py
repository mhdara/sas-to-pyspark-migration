# Databricks notebook source
# MAGIC %md
# MAGIC # 10_repayment_forecast
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928142931-up2j/10_repayment_forecast.sas`
# COMMAND ----------
# COMMAND ----------
# --------------------------------------------------------------
# Databricks notebook: 10_repayment_forecast.py
# --------------------------------------------------------------
# Imports & environment setup
# --------------------------------------------------------------
import pyspark.sql.functions as F
from pyspark.sql import Window
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# COMMAND ----------
# -----------------------------------------------------------------
# Parameters (replace the widget default with your own path if needed)
# -----------------------------------------------------------------
# In SAS the macro variable &root pointed to the base folder.
# In Databricks we expose the same via a widget.
# Example: dbutils.widgets.text("root", "/mnt/project")
root = dbutils.widgets.get("root")

# COMMAND ----------
# -----------------------------------------------------------------
# 1. Load source data (stg.loan_payments)
# -----------------------------------------------------------------
# SAS library reference  libname stg "&root/stg";
# In Databricks we read the table that lives under the same path.
# Adjust the catalog/schema.table reference to match your environment.
stg_tbl = f"{root}.stg.loan_payments"          # e.g. /mnt/project/stg/loan_payments
df_payments = spark.read.format("delta").table(stg_tbl)

# COMMAND ----------
# -----------------------------------------------------------------
# 2. PROC SQL equivalent → monthly aggregation (48 months)
#    SAS code:
#      intnx('month', payment_date, 0, 'b')  --> beginning of month
# -----------------------------------------------------------------
df_monthly = (
    df_payments
    # truncate the payment_date to the first day of its calendar month
    .withColumn("month", F.trunc(F.col("payment_date"), "month"))
    .groupBy("month")
    .agg(F.sum(F.col("payment_amount")).alias("total_payment"))
    .orderBy("month")
)

# COMMAND ----------
# Write the intermediate table out.monthly_repay (48 rows)
out_monthly_tbl = f"{root}.out.monthly_repay"
df_monthly.write.mode("overwrite").saveAsTable(out_monthly_tbl)

# COMMAND ----------
# -----------------------------------------------------------------
# 3. Double‑exponential smoothing (PROC FORECAST replacement)
#    - METHOD=EXPO  TREND=2   → Holt’s linear trend (additive)
#    - WEIGHT=0.2           → smoothing level α = 0.2
#    - NSTART=12            → first 12 observations are used to initialise
#    - LEAD=12              → forecast 12 periods ahead
# -----------------------------------------------------------------
# Convert to pandas for the modelling step (data set is tiny – 48 rows)
pdf_monthly = df_monthly.select("month", "total_payment") \
                         .toPandas() \
                         .sort_values("month") \
                         .reset_index(drop=True)

# COMMAND ----------
# Ensure the month column is a proper datetime (first‑day of month)
pdf_monthly["month"] = pd.to_datetime(pdf_monthly["month"])

# COMMAND ----------
# Build the Holt (double exponential) model with the exact parameters
holt_model = ExponentialSmoothing(
    endog=pdf_monthly["total_payment"],
    trend="add",                # TREND=2  → linear trend
    seasonal=None,
    damped_trend=False
).fit(
    smoothing_level=0.2,       # WEIGHT=0.2  (α)
    smoothing_slope=0.2,       # same value for the trend smoothing (β)
    optimized=False,           # respect the supplied weights, do not re‑optimize
    initialization_method="known",  # use the first NSTART observations verbatim
    initial_level=pdf_monthly["total_payment"].iloc[:12].mean(),   # rough init
    initial_slope=0.0          # SAS defaults to 0 when not supplied
)

# COMMAND ----------
# -----------------------------------------------------------------
# 3a. In‑sample fitted values (the “fitted” column)
# -----------------------------------------------------------------
pdf_monthly["fitted"] = holt_model.fittedvalues

# COMMAND ----------
# -----------------------------------------------------------------
# 3b. Forecast 12 months ahead (the “forecast” column)
# -----------------------------------------------------------------
forecast_vals = holt_model.forecast(12)

# COMMAND ----------
# Build a pandas DataFrame for the future periods
last_month = pdf_monthly["month"].iloc[-1]
future_months = pd.date_range(start=last_month + pd.offsets.MonthBegin(1),
                              periods=12,
                              freq="MS")   # month‑start frequency

# COMMAND ----------
pdf_forecast = pd.DataFrame({
    "month": future_months,
    "total_payment": pd.NA,       # no actuals for future rows
    "fitted": pd.NA,              # fitted values are not defined beyond the sample
    "forecast": forecast_vals
})

# COMMAND ----------
# -----------------------------------------------------------------
# 3c. Assemble the full OUTFULL table (actuals + fitted + forecasts)
# -----------------------------------------------------------------
pdf_outfull = pd.concat([pdf_monthly, pdf_forecast], ignore_index=True)

# COMMAND ----------
# -----------------------------------------------------------------
# 3d. OUTEST table – final internal state of the smoothing algorithm
# -----------------------------------------------------------------
# SAS PROC FORECAST writes several internal estimates; we capture the
# most important ones (final level and trend) plus the smoothing
# parameters that were forced.
out_est_dict = {
    "final_level": [holt_model.level[-1]],
    "final_trend": [holt_model.slope[-1]],
    "smoothing_level": [0.2],
    "smoothing_slope": [0.2],
    "n_observations": [len(pdf_monthly)]
}
pdf_outest = pd.DataFrame(out_est_dict)

# COMMAND ----------
# -----------------------------------------------------------------
# 4. Persist the results back to Databricks tables
# -----------------------------------------------------------------
# Helper to write a pandas DataFrame as a Spark table
def pandas_to_spark(df_pd, db_name, tbl_name, mode="overwrite"):
    spark_df = spark.createDataFrame(df_pd)
    spark_df.write.mode(mode).saveAsTable(f"{root}.{db_name}.{tbl_name}")

# COMMAND ----------
# out.fc  → full result (actual, fitted, forecast)
pandas_to_spark(pdf_outfull, "out", "fc")

# COMMAND ----------
# out.fc_est  → internal estimates
pandas_to_spark(pdf_outest, "out", "fc_est")

# COMMAND ----------
# -----------------------------------------------------------------
# 5. Display final tables for verification (Databricks UI)
# -----------------------------------------------------------------
display(spark.read.table(f"{root}.out.monthly_repay"))
display(spark.read.table(f"{root}.out.fc"))
display(spark.read.table(f"{root}.out.fc_est"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
