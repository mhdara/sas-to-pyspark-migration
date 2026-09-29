# Databricks notebook source
# MAGIC %md
# MAGIC # 09_payment_model
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/workspace/switch/switch_volume/input-20260928141958-i75y/09_payment_model.sas`
# COMMAND ----------
# COMMAND ----------
import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# COMMAND ----------
def loan_enriched():
    # Example: the enriched loans are stored as a Delta table at "/mnt/out/loan_enriched"
    return spark.read.format("delta").load("/mnt/out/loan_enriched")


# COMMAND ----------
def reg_est():
    # --------------------------------------------------------------
    # 1. Read the enriched loan data
    # --------------------------------------------------------------
    df = dlt.read("loan_enriched")

    # --------------------------------------------------------------
    # 2. Assemble the feature columns into a vector expected by Spark ML
    # --------------------------------------------------------------
    assembler = VectorAssembler(
        inputCols=["principal", "interest_rate", "term_months"],
        outputCol="features"
    )
    assembled = assembler.transform(df).select("monthly_payment", "features")

    # --------------------------------------------------------------
    # 3. Fit an OLS linear regression model with an intercept
    # --------------------------------------------------------------
    lr = LinearRegression(
        labelCol="monthly_payment",
        featuresCol="features",
        fitIntercept=True
    )
    model = lr.fit(assembled)

    # --------------------------------------------------------------
    # 4. Extract model parameters and fit statistics
    # --------------------------------------------------------------
    coeffs = model.coefficients.toArray()          # [b1, b2, b3]
    intercept = float(model.intercept)            # intercept
    rmse = float(model.summary.rootMeanSquaredError)  # root mean squared error

    # --------------------------------------------------------------
    # 5. Build a one‑row DataFrame that matches the SAS OUTEST layout
    # --------------------------------------------------------------
    result = spark.createDataFrame(
        [
            (
                intercept,
                float(coeffs[0]),   # coefficient for principal (b1)
                float(coeffs[1]),   # coefficient for interest_rate (b2)
                float(coeffs[2]),   # coefficient for term_months (b3)
                rmse
            )
        ],
        schema=["intercept", "b1_principal", "b2_interest_rate", "b3_term_months", "rmse"]
    )

    return result
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
