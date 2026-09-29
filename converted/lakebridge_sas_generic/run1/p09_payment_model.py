# Databricks notebook source
# MAGIC %md
# MAGIC # 09_payment_model
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928145005-uq1l/09_payment_model.sas`
# COMMAND ----------
# COMMAND ----------
# ------------------------------------------------------------
# 09_payment_model.py  –  Linear regression of monthly_payment
# ------------------------------------------------------------
# SAS Summary:
#   PROC REG fits: monthly_payment = intercept + b1*principal + b2*interest_rate + b3*term_months
#   INPUT  : out.loan_enriched   (≈ 799 rows)
#   OUTPUT : out.reg_est – one‑row data set with intercept, three coefficients, and RMSE
# ------------------------------------------------------------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# COMMAND ----------
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# ------------------------------------------------------------------
# 1. Resolve SAS libname reference
#    SAS: libname out "&root/out";
#    In Databricks we map the macro variable `root` to a notebook widget.
# ------------------------------------------------------------------
# Create a widget (if not already defined) to pass the root path, e.g.:
# dbutils.widgets.text("root", "/Volumes/project/out")
root_path = dbutils.widgets.get("root")          # e.g. "/Volumes/project/out"
out_schema = "out"                               # Databricks database name for the libref

# COMMAND ----------
# ------------------------------------------------------------------
# 2. Read the enriched loan data
#    SAS: data=out.loan_enriched
# ------------------------------------------------------------------
df_loans = spark.read.table(f"{out_schema}.loan_enriched") \
                .select(
                    F.col("monthly_payment").cast("double"),
                    F.col("principal").cast("double"),
                    F.col("interest_rate").cast("double"),
                    F.col("term_months").cast("double")
                ) \
                .cache()      # reused for model fitting and summary extraction

# COMMAND ----------
# ------------------------------------------------------------------
# 3. Assemble feature vector (principal, interest_rate, term_months)
# ------------------------------------------------------------------
assembler = VectorAssembler(
    inputCols=["principal", "interest_rate", "term_months"],
    outputCol="features"
)
df_features = assembler.transform(df_loans).select("monthly_payment", "features")

# COMMAND ----------
# ------------------------------------------------------------------
# 4. Fit an OLS linear regression with an intercept
#    SAS PROC REG automatically includes an intercept unless SPECIFIED otherwise.
# ------------------------------------------------------------------
lr = LinearRegression(
    labelCol="monthly_payment",
    featuresCol="features",
    fitIntercept=True,
    solver="normal"          # use the normal equations to guarantee identical OLS solution
)

# COMMAND ----------
lr_model = lr.fit(df_features)

# COMMAND ----------
# ------------------------------------------------------------------
# 5. Extract coefficients, intercept, and RMSE (root mean squared error)
# ------------------------------------------------------------------
coefficients = lr_model.coefficients.toArray().tolist()   # [b1, b2, b3]
intercept = float(lr_model.intercept)                    # intercept

# COMMAND ----------
training_summary = lr_model.summary
rmse = float(training_summary.rootMeanSquaredError)      # RMS error on the training data

# COMMAND ----------
# ------------------------------------------------------------------
# 6. Build a one‑row DataFrame that mimics the SAS OUTEST data set
#    Column order in SAS OUTEST (default) is:
#       _TYPE_, _DEPVAR_, INTERCEPT, principal, interest_rate, term_months, _RMSE_
#    For simplicity we keep only the numeric parts required by downstream code.
# ------------------------------------------------------------------
out_schema_fields = [
    "intercept",
    "principal_coef",
    "interest_rate_coef",
    "term_months_coef",
    "rmse"
]

# COMMAND ----------
out_row = [(intercept,
            coefficients[0],
            coefficients[1],
            coefficients[2],
            rmse)]

# COMMAND ----------
df_out_est = spark.createDataFrame(out_row, schema=out_schema_fields)

# COMMAND ----------
# ------------------------------------------------------------------
# 7. Write the result to the target table (equivalent of OUT.REG_EST)
# ------------------------------------------------------------------
# Overwrite any existing content to mimic SAS behaviour (PROC REG OUTEST= replaces the data set)
df_out_est.write.mode("overwrite").saveAsTable(f"{out_schema}.reg_est")

# COMMAND ----------
# ------------------------------------------------------------------
# 8. Optional: display the coefficients inside the notebook
# ------------------------------------------------------------------
display(df_out_est)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
