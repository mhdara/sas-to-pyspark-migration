import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("06_macro_library").getOrCreate()

def summarize(ds, class_var, var, out):
    df = spark.read.parquet(ds)
    window_spec = Window.partitionBy(class_var).orderBy(F.desc(var))
    df = df.groupBy(class_var).agg(
        F.count(var).alias("n"),
        F.sum(var).alias("total"),
        F.avg(var).alias("avg")
    )
    df.write.mode("overwrite").parquet(out)

def flag_high(ds, var, threshold, out):
    df = spark.read.parquet(ds)
    df = df.withColumn("high_flag", (F.col(var) > threshold).cast("integer"))
    df.write.mode("overwrite").parquet(out)
