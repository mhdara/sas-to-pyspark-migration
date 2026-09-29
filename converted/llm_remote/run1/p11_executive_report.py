import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("11_executive_report").getOrCreate()

CTRL = "data/parquet/ctrl"
STG  = "data/parquet/stg"
OUT  = "data/parquet/py_out/llm_remote/run1"

# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------
macro_parameters = spark.read.parquet(f"{CTRL}/macro_parameters.parquet")
loan_enriched    = spark.read.parquet(f"{OUT}/loan_enriched")
loan_status      = spark.read.parquet(f"{OUT}/loan_status")
fc               = spark.read.parquet(f"{OUT}/fc")
monthly_repay    = spark.read.parquet(f"{OUT}/monthly_repay")

# ---------------------------------------------------------------------------
# proc sql noprint; select param_value into :dpd_threshold trimmed ...
# ---------------------------------------------------------------------------
_row = (macro_parameters
        .filter(F.col("param_name") == F.lit("DPD_THRESHOLD"))
        .select("param_value")
        .first())
dpd_threshold_raw = None if _row is None else _row[0]
# the parameter is stored as text in the settings table -> numeric comparison in SAS
dpd_threshold = float(str(dpd_threshold_raw).strip())

# ---------------------------------------------------------------------------
# create table out.exec_report
#   left join on loan_id, group by segment_name
# ---------------------------------------------------------------------------
e = loan_enriched.alias("e")
s = loan_status.select("loan_id", "max_dpd").alias("s")

joined = e.join(s, F.col("e.loan_id") == F.col("s.loan_id"), "left")

exec_report = (joined
    .groupBy(F.col("e.segment_name").alias("segment_name"))
    .agg(
        F.countDistinct(F.col("e.loan_id")).alias("n_loans"),
        # SAS: sum(s.max_dpd > &dpd_threshold) -> missing max_dpd yields 0 (missing is not > x)
        F.sum(
            F.when(F.col("s.max_dpd").isNull(), F.lit(0))
             .when(F.col("s.max_dpd") > F.lit(dpd_threshold), F.lit(1))
             .otherwise(F.lit(0))
        ).alias("n_delinquent_loans"),
        F.sum(F.col("e.principal")).alias("total_principal"),
    )
)

exec_report.write.mode("overwrite").parquet(f"{OUT}/exec_report")

# ---------------------------------------------------------------------------
# create table out.exec_forecast
#   keep only the FORECAST rows after the last observed repayment month
# ---------------------------------------------------------------------------
max_month_row = monthly_repay.select(F.max(F.col("month")).alias("m")).first()
max_month = None if max_month_row is None else max_month_row[0]

fc_f = fc.filter(F.col("_type_") == F.lit("FORECAST"))
if max_month is None:
    # SAS: month > . (missing) is true for every non-missing month, false for missing month
    fc_f = fc_f.filter(F.col("month").isNotNull())
else:
    fc_f = fc_f.filter(F.col("month").isNotNull() & (F.col("month") > F.lit(max_month)))

exec_forecast = fc_f.select(
    F.col("month"),
    F.col("total_payment").alias("forecast_payment"),
)

exec_forecast.write.mode("overwrite").parquet(f"{OUT}/exec_forecast")

spark.stop()
