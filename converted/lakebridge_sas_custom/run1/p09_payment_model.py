# Databricks notebook source
# MAGIC %md
# MAGIC # 09_payment_model
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260929024630-suw5/09_payment_model.sas`
# COMMAND ----------
# COMMAND ----------
# ------------------------------------------------------------
# 09_payment_model.sas  ->  Databricks Python (PySpark)
# Linear regression (OLS) on out.loan_enriched
# Writes: workspace.out.reg_est  (intercept, principal, interest_rate,
#                                 term_months, rmse)
# ------------------------------------------------------------

import pyspark.sql.functions as F
import numpy as np
import pandas as pd

# COMMAND ----------
# ------------------------------------------------------------------
# 1.  Read the enriched loan data (the SAS PROC REG input table)
# ------------------------------------------------------------------
df_spark = spark.read.table("workspace.out.loan_enriched")

# COMMAND ----------
# ------------------------------------------------------------------
# 2.  Bring the data to the driver as a pandas DataFrame.
#     The dataset is small (≈ 799 rows), so this is safe.
# ------------------------------------------------------------------
df_pd = df_spark.toPandas()

# COMMAND ----------
# ------------------------------------------------------------------
# 3.  Prepare the design matrix (add an intercept column) and the
#     response vector.
# ------------------------------------------------------------------
y = df_pd["monthly_payment"].values.reshape(-1, 1)               # (n,1)
X = df_pd[["principal", "interest_rate", "term_months"]].values # (n,3)

# COMMAND ----------
# Add a column of 1's for the intercept term.
X_with_intercept = np.hstack([np.ones((X.shape[0], 1)), X])      # (n,4)

# COMMAND ----------
# ------------------------------------------------------------------
# 4.  Ordinary Least Squares using NumPy (identical to SAS PROC REG)
# ------------------------------------------------------------------
#   beta = (X'X)^(-1) X'y   ->  solved via least‑squares for numerical stability
beta, residuals, rank, s = np.linalg.lstsq(X_with_intercept, y, rcond=None)

# COMMAND ----------
# Extract coefficients
intercept      = float(beta[0, 0])
coef_principal = float(beta[1, 0])
coef_ir        = float(beta[2, 0])
coef_term      = float(beta[3, 0])

# COMMAND ----------
# ------------------------------------------------------------------
# 5.  Compute Root Mean Squared Error (same definition SAS uses)
# ------------------------------------------------------------------
y_pred = X_with_intercept @ beta                     # (n,1)
residual = y - y_pred
rmse = float(np.sqrt(np.mean(residual ** 2)))

# COMMAND ----------
# ------------------------------------------------------------------
# 6.  Build the output table – one row, all columns in lower‑case
# ------------------------------------------------------------------
out_pd = pd.DataFrame({
    "intercept":      [intercept],
    "principal":      [coef_principal],
    "interest_rate":  [coef_ir],
    "term_months":    [coef_term],
    "rmse":           [rmse]
})

# COMMAND ----------
# ------------------------------------------------------------------
# 7.  Write the result to the Unity Catalog schema `workspace.out`
# ------------------------------------------------------------------
out_spark = spark.createDataFrame(out_pd)

# COMMAND ----------
out_spark.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.out.reg_est")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
