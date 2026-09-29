import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("04_payment_delinquency").getOrCreate()

CTRL = "data/parquet/ctrl"
STG  = "data/parquet/stg"
OUT  = "data/parquet/py_out/llm_remote/run1"

# ------------------------------------------------------------------
# 1. proc sql : select param_value into :dpd_threshold trimmed
# ------------------------------------------------------------------
macro_parameters = spark.read.parquet(f"{CTRL}/macro_parameters.parquet")

_row = (macro_parameters
        .filter(F.col("param_name") == F.lit("DPD_THRESHOLD"))
        .select("param_value")
        .first())
dpd_threshold = str(_row[0]).strip() if _row is not None else None   # text value, e.g. "30"
dpd_threshold_num = float(dpd_threshold)                             # used in a numeric comparison

# ------------------------------------------------------------------
# 2. proc sort nodupkey  by loan_id payment_date  (keeps the FIRST row)
# ------------------------------------------------------------------
loan_payments = spark.read.parquet(f"{STG}/loan_payments.parquet")

# preserve the physical input order so that "first row of the key" is deterministic
pay_in = loan_payments.withColumn("_src_ord", F.monotonically_increasing_id())

w_dup = Window.partitionBy("loan_id", "payment_date").orderBy(F.col("_src_ord").asc())
pay_sorted = (pay_in
              .withColumn("_rn_dup", F.row_number().over(w_dup))
              .filter(F.col("_rn_dup") == 1)
              .drop("_rn_dup"))

# ------------------------------------------------------------------
# 3. data step : BY loan_id, sum statement, RETAIN max_dpd
# ------------------------------------------------------------------
# rows are processed in BY order: loan_id, payment_date (then original order as tiebreak)
w_run = (Window.partitionBy("loan_id")
         .orderBy(F.col("payment_date").asc(), F.col("_src_ord").asc())
         .rowsBetween(Window.unboundedPreceding, 0))

w_last = Window.partitionBy("loan_id").orderBy(F.col("payment_date").desc(), F.col("_src_ord").desc())

delinq = (pay_sorted
          # cum_paid + payment_amount : running total, missing treated as 0, reset at first.loan_id
          .withColumn("cum_paid",
                      F.sum(F.coalesce(F.col("payment_amount"), F.lit(0.0))).over(w_run))
          # max_dpd = max(max_dpd, days_past_due) with max_dpd initialised to 0 at first.loan_id
          # SAS max() ignores missing values, so a null days_past_due leaves max_dpd unchanged
          .withColumn("_run_max_dpd", F.max(F.col("days_past_due")).over(w_run))
          .withColumn("max_dpd",
                      F.when(F.col("_run_max_dpd").isNull(), F.lit(0.0))
                       .otherwise(F.greatest(F.col("_run_max_dpd").cast("double"), F.lit(0.0))))
          .drop("_run_max_dpd")
          # delinquent_flag = (days_past_due > &dpd_threshold)
          # a missing days_past_due is SMALLER than every number -> flag = 0
          .withColumn("delinquent_flag",
                      F.when(F.col("days_past_due").isNull(), F.lit(0))
                       .when(F.col("days_past_due").cast("double") > F.lit(dpd_threshold_num), F.lit(1))
                       .otherwise(F.lit(0)).cast("int"))
          # period_id = put(payment_date, yymmn6.)  -> length $6
          .withColumn("period_id", F.date_format(F.col("payment_date"), "yyyyMM").cast("string"))
          # last.loan_id marker for the second output table
          .withColumn("_last_in_loan", F.when(F.row_number().over(w_last) == 1, F.lit(1)).otherwise(F.lit(0)))
          )

delinq.cache()

# output out.delinquency : every payment row
out_delinquency = delinq.drop("_src_ord", "_last_in_loan")

# output out.loan_status : only the last row of each loan, keep=loan_id cum_paid max_dpd
out_loan_status = (delinq
                   .filter(F.col("_last_in_loan") == 1)
                   .select("loan_id", "cum_paid", "max_dpd"))

out_delinquency.write.mode("overwrite").parquet(f"{OUT}/delinquency")
out_loan_status.write.mode("overwrite").parquet(f"{OUT}/loan_status")

spark.stop()
