import os, sys
from pyspark.sql import SparkSession, functions as F, Window
import pandas as pd
import statsmodels.api as sm

spark = SparkSession.builder.master("local[*]").appName("09_payment_model").getOrCreate()

# Read the input data
df = spark.read.parquet("data/parquet/py_out/llm_local/run1/loan_enriched.parquet")

# Convert the DataFrame to a Pandas DataFrame
pdf = df.select("monthly_payment", "principal", "interest_rate", "term_months").toPandas()

# Define the independent variables and the dependent variable
X = pdf[["principal", "interest_rate", "term_months"]]
y = pdf["monthly_payment"]

# Add a constant to the independent variables (intercept)
X = sm.add_constant(X)

# Fit the ordinary least squares model
model = sm.OLS(y, X).fit()

# Extract the coefficients and the root mean squared error
coefficients = model.params
rmse = model.mse_resid**0.5

# Create a DataFrame with the results
results_df = pd.DataFrame({
    "intercept": [coefficients[0]],
    "principal": [coefficients[1]],
    "interest_rate": [coefficients[2]],
    "term_months": [coefficients[3]],
    "_rmse_": [rmse]
})

# Convert the Pandas DataFrame back to a Spark DataFrame
results_spark_df = spark.createDataFrame(results_df)

# Write the output to a Parquet file
results_spark_df.write.mode("overwrite").parquet("data/parquet/py_out/llm_local/run1/reg_est")
