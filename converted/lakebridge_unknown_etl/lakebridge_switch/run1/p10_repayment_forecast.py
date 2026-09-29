# Databricks notebook source
# MAGIC %md
# MAGIC # 10_repayment_forecast
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928141958-i75y/10_repayment_forecast.sas`
# COMMAND ----------
# COMMAND ----------
import dlt
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

# COMMAND ----------
# The following import requires the `statsmodels` library to be available on the cluster.
# It provides the Holt‑Winters exponential smoothing implementation needed to
# reproduce the SAS PROC FORECAST double‑exponential (trend=2) smoothing.
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import pandas as pd

# COMMAND ----------
# -------------------------------------------------------------------------
# Helper: convert a payment date to the first day of its calendar month.
# Equivalent to SAS INTNX('month', payment_date, 0, 'b').
# -------------------------------------------------------------------------
def month_start(col):
    return F.date_trunc("month", col)

# COMMAND ----------
def monthly_repay():
    # Read the staging table that contains the raw loan payment records.
    # The path is built from the `root` variable that should be defined as a
    # notebook/cluster configuration (e.g. dbutils.widgets.get("root")).
    root = spark.conf.get("root")          # e.g. "/mnt/data"
    df = spark.read.format("delta").load(f"{root}/stg/loan_payments")

    # Ensure the payment_date column is a proper date type.
    df = df.withColumn("payment_date", F.to_date("payment_date"))

    # Group by month‑start and sum the payment amounts.
    result = (
        df
        .groupBy(month_start(F.col("payment_date")).alias("month"))
        .agg(F.sum("payment_amount").alias("total_payment"))
        .orderBy("month")
    )
    return result


# COMMAND ----------
def fc():
    # Pull the aggregated monthly data generated above.
    monthly_df = dlt.read("monthly_repay")

    # -----------------------------------------------------------------
    # Pandas UDF: perform the Holt‑Winters fit on the entire series.
    # The UDF is defined as a GROUPED_MAP because we need to return a
    # DataFrame with a different number of rows (original + 12 forecast rows).
    # -----------------------------------------------------------------
    schema = T.StructType([
        T.StructField("month",      T.DateType(),    nullable=False),
        T.StructField("actual",     T.DoubleType(), nullable=True ),
        T.StructField("fitted",     T.DoubleType(), nullable=True ),
        T.StructField("forecast",   T.DoubleType(), nullable=True )
    ])

    @F.pandas_udf(schema, F.PandasUDFType.GROUPED_MAP)
    def holt_forecast(pdf: pd.DataFrame) -> pd.DataFrame:
        # Ensure the data is sorted by month (important for time‑series).
        pdf = pdf.sort_values("month").reset_index(drop=True)

        # Extract the series to be modelled.
        ts = pdf["total_payment"].astype(float)

        # Fit Holt's linear trend with the exact smoothing parameters used in SAS.
        #  - smoothing_level  = 0.2  (WEIGHT)
        #  - smoothing_trend  = None (let the algorithm derive it, same as SAS)
        #  - optimized=False  ensures the weight is not re‑estimated.
        #  - initialization_method="estimated" mimics SAS's NSTART=12 behaviour.
        model = ExponentialSmoothing(
            ts,
            trend="add",
            damped_trend=False,
            initialization_method="estimated"
        )
        fit = model.fit(
            smoothing_level=0.2,
            smoothing_trend=None,
            optimized=False,
            n_init=12          # corresponds to NSTART=12 in SAS
        )

        # -----------------------------------------------------------------
        # 1) Fitted values for the historical period
        # -----------------------------------------------------------------
        fitted_vals = fit.fittedvalues

        # -----------------------------------------------------------------
        # 2) 12‑month forward forecasts
        # -----------------------------------------------------------------
        forecast_vals = fit.forecast(12)

        # -----------------------------------------------------------------
        # Build the result DataFrame:
        #   - Historical rows contain actual, fitted, and NULL forecast.
        #   - Future rows contain NULL actual/fitted and the forecast value.
        # -----------------------------------------------------------------
        # Historical part
        hist = pd.DataFrame({
            "month":    pdf["month"],
            "actual":   ts,
            "fitted":   fitted_vals,
            "forecast": [None] * len(ts)
        })

        # Future part: generate month‑start dates for the 12 lead months.
        last_month = pdf["month"].iloc[-1]
        future_months = pd.date_range(
            start=last_month + pd.offsets.MonthBegin(1),
            periods=12,
            freq="MS"
        )
        fut = pd.DataFrame({
            "month":    future_months,
            "actual":   [None] * 12,
            "fitted":   [None] * 12,
            "forecast": forecast_vals
        })

        return pd.concat([hist, fut], ignore_index=True)

    # Apply the UDF. All rows belong to a single group (hence the dummy key).
    result = monthly_df.groupBy(F.lit(1)).apply(holt_forecast)
    return result


# COMMAND ----------
def fc_est():
    monthly_df = dlt.read("monthly_repay")

    schema = T.StructType([
        T.StructField("model",            T.StringType(),  nullable=False),
        T.StructField("smoothing_level",  T.DoubleType(),  nullable=False),
        T.StructField("smoothing_trend",  T.DoubleType(),  nullable=False),
        T.StructField("initial_level",   T.DoubleType(),  nullable=False),
        T.StructField("initial_trend",   T.DoubleType(),  nullable=False)
    ])

    @F.pandas_udf(schema, F.PandasUDFType.GROUPED_AGG)
    def model_estimator(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.sort_values("payment_date").reset_index(drop=True)
        ts = pdf["total_payment"].astype(float)

        model = ExponentialSmoothing(
            ts,
            trend="add",
            damped_trend=False,
            initialization_method="estimated"
        )
        fit = model.fit(
            smoothing_level=0.2,
            smoothing_trend=None,
            optimized=False,
            n_init=12
        )

        return pd.DataFrame({
            "model":            ["Holt_Linear"],
            "smoothing_level":  [fit.params["smoothing_level"]],
            "smoothing_trend":  [fit.params.get("smoothing_trend", None)],
            "initial_level":    [fit.params["initial_level"]],
            "initial_trend":    [fit.params["initial_trend"]]
        })

    # As before, use a dummy key to force a single group.
    result = monthly_df.groupBy(F.lit(1)).agg(F.first(F.lit(1))).select("*")  # placeholder
    result = monthly_df.groupBy(F.lit(1)).apply(model_estimator)
    return result
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
