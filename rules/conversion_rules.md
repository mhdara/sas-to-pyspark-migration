# conversion_rules.md - SAS -> PySpark rules (version 1.1: R16 also defines fc_est, set before any
# LLM converted program 10)
# Each "## " block is injected into the conversion prompt ONLY when one of its triggers
# (case-insensitive regular expressions) is found in the SAS program. R0 is always included.

## R0 General contract
triggers: .*
- Target: a standalone Python 3 script using the PySpark DataFrame API. No RDDs.
- Start with: import os, sys; from pyspark.sql import SparkSession, functions as F, Window;
  spark = SparkSession.builder.master("local[*]").appName("<program_id>").getOrCreate()
- Paths come ONLY from the LIBRARY PATHS given in the prompt; paths are relative to the project root.
- Read a source table with spark.read.parquet("<library path>/<table>.parquet") for ctrl/stg,
  and spark.read.parquet("<library path>/<table>") for tables written by other converted programs.
- Write every SAS output table with df.write.mode("overwrite").parquet("<out path>/<table>").
- Keep SAS output column names exactly, in lower case. Flags are integers 0/1, never booleans.
- Never drop logic. If something cannot be converted, keep a comment explaining it and add "# TODO MANUAL".
- Output Python code only (no explanations, no markdown fences).

## R1 DATA step and IF/ELSE
triggers: \bdata\s+\w+\.\w+, \bif\b
- The implicit row loop becomes column expressions (withColumn / select).
- IF / ELSE IF / ELSE becomes F.when(...).when(...).otherwise(...) in the same order.

## R3 Missing values in comparisons
triggers: \bif\b.*[<>]=?, \bmissing\(, \bwhere\b
- In SAS a missing number is SMALLER than every number and a comparison is always true/false.
  Example: SAS "if income < 20000" is TRUE when income is missing. In Spark the test is null.
  Always add an explicit branch: F.when(F.col(c).isNull() | (F.col(c) < 20000), ...).
- missing(x) becomes F.col("x").isNull() cast to int.

## R4 Sum statement and RETAIN
triggers: \bretain\b, ^\s*\w+\s*\+\s*[\w.]+\s*;
- "x + y;" keeps a running total and treats a missing y as 0: use
  F.sum(F.coalesce(F.col("y"), F.lit(0))).over(w) with
  w = Window.partitionBy(<BY key>).orderBy(<sort columns>).rowsBetween(Window.unboundedPreceding, 0).
- A running max becomes F.max(...).over(w). Always partition the window by the BY key.

## R6 BY groups, FIRST. and LAST.
triggers: \bfirst\.\w+, \blast\.\w+, \bby\b
- Rows are ordered by the BY variables (and the sort order of the input).
- LAST.key row = F.row_number() over Window.partitionBy(key).orderBy(F.desc(<order>)) == 1.

## R8 PROC SORT NODUPKEY
triggers: nodupkey
- SAS keeps the FIRST row of each key in the current order. dropDuplicates keeps an arbitrary row.
  Use row_number() over Window.partitionBy(<by vars>).orderBy(<by vars>) == 1.

## R10 PROC SQL
triggers: proc\s+sql
- Convert joins with the DataFrame API; keep join types (inner/left) and column names.
- "select ... into :var" reads a value from a table into a macro variable: use
  value = spark.read.parquet(...).filter(...).select(...).first()[0]  (a Python variable).
- "into :list separated by ' '" becomes a Python list from .collect(), in the SQL order.
- sum(condition) counts true rows: F.sum(F.when(cond, 1).otherwise(0)); a null never satisfies it.
- count(distinct x) becomes F.countDistinct("x").

## R11 PROC MEANS / SUMMARY
triggers: proc\s+means, proc\s+summary
- With NWAY + CLASS: df.groupBy(class).agg(...). N counts NON-MISSING values: F.count(col).
- output out=... n=a sum=b mean=c -> columns a, b, c (drop _type_ and _freq_ when the SAS code drops them).

## R12 PROC FREQ
triggers: proc\s+freq
- "tables a*b / out=t" -> groupBy(a, b).count() renamed to count, plus percent = 100 * count / total rows.
  Rows with a missing a or b are excluded (SAS default).

## R13 PROC FORMAT + PUT
triggers: proc\s+format, \bput\(
- No PySpark equivalent. Rebuild the format as an F.when chain in the same order.
  "low -< 20000" means < 20000; "80000 - high" means >= 80000; OTHER also receives missing values.

## R15 PROC REG
triggers: proc\s+reg
- Collect the (small) data to pandas and fit ordinary least squares with statsmodels (with intercept).
- Write one row with columns: intercept, <each regressor>, _rmse_ (sqrt of the mean squared error with n-p-1 df).

## R16 PROC FORECAST
triggers: proc\s+forecast
- Do NOT use a library forecasting function. Use the project module:
  from forecast import brown_expo   # fitted, future, states = brown_expo(y, weight, nstart, lead)
- Build output table fc with columns: month, _type_ ("ACTUAL" or "FORECAST"), _lead_, total_payment,
  where FORECAST rows hold the fitted values for history (_lead_=0) and the future values (_lead_=1..lead).
- Build output table fc_est with columns: _type_, month, total_payment and one row per final state
  _type_ = "S1", "S2", "CONSTANT", "LINEAR" (from the states dict), month = the last history month.
- The 95% confidence limits (L95/U95 rows) and fit statistics are not reproduced.

## R17 Dates and INTNX
triggers: intnx, intck, yymm, \bformat\b.*yymmdd
- SAS dates are days since 1960-01-01; after migration they are DATE columns.
- intnx('month', d, 0, 'b') -> F.trunc(d, "month"). put(d, yymmn6.) -> F.date_format(d, "yyyyMM").

## R20 Macros, macro variables and nested macros
triggers: %macro, %let, &\w+, %do
- A %macro becomes a Python function with the same parameters (keyword arguments, same defaults).
- A nested macro call becomes a function call inside a function; %do loops become for loops.
- Table names built from macro variables (out.x_&pid) become f-strings (f"x_{pid}").

## R23 %INCLUDE
triggers: %include
- %include "<...>/NN_name.sas" becomes: from pNN_name import *   (the included program is already
  converted to converted/pNN_name.py and is on the PYTHONPATH). Do not copy its code.
