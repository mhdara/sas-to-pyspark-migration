import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("09_payment_model").getOrCreate()

OUT = "data/parquet/py_out/llm_remote/run1"

# Reads: out.loan_enriched (written by program 02)
loan_enriched = spark.read.parquet(f"{OUT}/loan_enriched")

dep = "monthly_payment"
regressors = ["principal", "interest_rate", "term_months"]

# PROC REG uses only complete cases (listwise deletion of missing model variables)
model_df = loan_enriched.select([F.col(c).cast("double").alias(c) for c in [dep] + regressors])
for c in [dep] + regressors:
    model_df = model_df.filter(F.col(c).isNotNull())

pdf = model_df.toPandas()

import numpy as np
import statsmodels.api as sm

X = sm.add_constant(pdf[regressors].astype(float), has_constant="add")
y = pdf[dep].astype(float)
fit = sm.OLS(y, X).fit()

params = fit.params
intercept = float(params["const"])
coefs = {c: float(params[c]) for c in regressors}

# _RMSE_ : sqrt of the mean squared error with n - p - 1 degrees of freedom
resid = np.asarray(fit.resid, dtype=float)
n = len(resid)
p = len(regressors)
rmse = float(np.sqrt(np.sum(resid ** 2) / (n - p - 1)))

row = {"intercept": intercept}
for c in regressors:
    row[c] = coefs[c]
row["_rmse_"] = rmse

from pyspark.sql.types import StructType, StructField, DoubleType

schema = StructType(
    [StructField("intercept", DoubleType(), True)]
    + [StructField(c, DoubleType(), True) for c in regressors]
    + [StructField("_rmse_", DoubleType(), True)]
)

reg_est = spark.createDataFrame([tuple([row["intercept"]] + [row[c] for c in regressors] + [row["_rmse_"]])], schema=schema)

reg_est.write.mode("overwrite").parquet(f"{OUT}/reg_est")

spark.stop()
