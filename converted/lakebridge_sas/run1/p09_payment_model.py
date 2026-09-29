# Databricks notebook source
# MAGIC %md
# MAGIC # 09_payment_model
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928142931-up2j/09_payment_model.sas`
# COMMAND ----------
# COMMAND ----------
# ------------------------------------------------------------
# Linear regression (OLS) equivalent of SAS PROC REG
# ------------------------------------------------------------
# SAS macro variable &root is mapped to a Databricks widget.
# The SAS libname OUT points to the folder “&root/out”.
# ----------------------------------------------------------------
# Reads : out.loan_enriched  (Delta table / Parquet folder)
# Writes: out.reg_est        (single‑row table with coefficients & RMSE)
# ----------------------------------------------------------------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# COMMAND ----------
# ----------------------------------------------------------------
# 1. Resolve the macro variable "&root"
# ----------------------------------------------------------------
# In a Databricks notebook the widget must be created earlier, e.g.:
# dbutils.widgets.text("root", "/mnt/project")
root_path = dbutils.widgets.get("root")          # e.g. "/mnt/project"

# COMMAND ----------
# ----------------------------------------------------------------
# 2. Load the enriched loan data
# ----------------------------------------------------------------
# The SAS libref OUT resolves to the folder  <root>/out
loan_enriched_path = f"{root_path}/out/loan_enriched"

# COMMAND ----------
# Adjust the format (Delta, Parquet, CSV…) to match the actual source.
# Here we assume a Delta table stored in that folder.
df = spark.read.format("delta").load(loan_enriched_path)

# COMMAND ----------
# ----------------------------------------------------------------
# 3. Prepare the feature vector as required by Spark ML
# ----------------------------------------------------------------
feature_cols = ["principal", "interest_rate", "term_months"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# COMMAND ----------
df_features = assembler.transform(df).select(
    F.col("monthly_payment").alias("label"),
    "features"
)

# COMMAND ----------
# ----------------------------------------------------------------
# 4. Fit an ordinary‑least‑squares linear regression model
# ----------------------------------------------------------------
# Using the “normal” solver gives the exact OLS solution (no regularisation).
lr = (
    LinearRegression(
        featuresCol="features",
        labelCol="label",
        fitIntercept=True,          # include intercept (same as SAS default)
        regParam=0.0,               # no regularisation
        elasticNetParam=0.0,        # pure L2 (but regParam=0 → OLS)
        solver="normal"             # solve by normal equations → exact OLS
    )
)

# COMMAND ----------
model = lr.fit(df_features)

# COMMAND ----------
# ----------------------------------------------------------------
# 5. Extract coefficients, intercept and training RMSE
# ----------------------------------------------------------------
coefficients = model.coefficients.toArray().tolist()   # [b1, b2, b3] order matches feature_cols
intercept   = float(model.intercept)                   # scalar

# COMMAND ----------
# Spark provides the root‑mean‑squared‑error on the training data.
rmse = float(model.summary.rootMeanSquaredError)

# COMMAND ----------
# ----------------------------------------------------------------
# 6. Build the one‑row DataFrame that mimics OUT.REG_EST
# ----------------------------------------------------------------
# SAS PROC REG with OUTEST creates a data set having columns:
#   _TYPE_, _DEPVAR_, _MODEL_, intercept, principal, interest_rate, term_months,
#   _RSQ_, _ADJRSQ_, _RMSE_, etc.
# For the purpose of this conversion we keep only the essential columns:
#   intercept, principal, interest_rate, term_months, rmse.
# If the downstream code expects the exact column names, rename accordingly.

reg_est_schema = [
    ("intercept",      F.lit(intercept)),
    ("principal_coef", F.lit(coefficients[0])),
    ("interest_rate_coef", F.lit(coefficients[1])),
    ("term_months_coef",    F.lit(coefficients[2])),
    ("rmse",           F.lit(rmse))
]

# COMMAND ----------
df_reg_est = spark.createDataFrame(
    [(
        intercept,
        coefficients[0],
        coefficients[1],
        coefficients[2],
        rmse
    )],
    schema=["intercept",
            "principal_coef",
            "interest_rate_coef",
            "term_months_coef",
            "rmse"]
)

# COMMAND ----------
# ----------------------------------------------------------------
# 7. Write the result back to the OUT library
# ----------------------------------------------------------------
reg_est_path = f"{root_path}/out/reg_est"

# COMMAND ----------
# Overwrite any existing file – matches SAS behavior of replacing OUTEST.
df_reg_est.write.mode("overwrite").format("delta").save(reg_est_path)

# COMMAND ----------
# Optionally register as a table for easy SQL access
spark.sql(f"CREATE OR REPLACE TABLE out.reg_est USING DELTA LOCATION '{reg_est_path}'")

# COMMAND ----------
# ----------------------------------------------------------------
# 8. Show the coefficients (Databricks visualisation)
# ----------------------------------------------------------------
display(df_reg_est)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
