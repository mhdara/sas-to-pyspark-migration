import os, sys, inspect
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql import DataFrame

spark = SparkSession.builder.master("local[*]").appName("08_period_driver").getOrCreate()

# %include "&root/programs/06_macro_library.sas"
from p06_macro_library import *  # noqa: F401,F403

# ---------------------------------------------------------------------------
# library paths (relative to the project root)
# ---------------------------------------------------------------------------
CTRL = "data/parquet/ctrl"
STG = "data/parquet/stg"
OUT = "data/parquet/py_out/llm_remote/run1"

# filename mprint / options mprint mfile : SAS-only logging of generated code,
# no data effect in Python.
# TODO MANUAL (only if the generated-code log file is really required)

# ---------------------------------------------------------------------------
# proc sql : select period_id into :period_list separated by ' '
#            from ctrl.reporting_periods where active_flag = 1 order by period_id
# ---------------------------------------------------------------------------
period_list = [
    r[0]
    for r in (
        spark.read.parquet(f"{CTRL}/reporting_periods.parquet")
        .filter(F.col("active_flag") == 1)
        .orderBy(F.col("period_id").asc())
        .select("period_id")
        .collect()
    )
]


# ---------------------------------------------------------------------------
# helper : call %summarize from program 06 whatever its converted signature is.
# The SAS call is  %summarize(ds=, class=, var=, out=) ; "class" is a reserved
# word in Python so the converted macro may use any spelling (class_, cls,
# class_vars, by_vars, ...).  The parameter names are discovered at run time so
# that the CLASS variable really reaches the macro (an empty CLASS list made the
# previous run fail inside the groupBy/orderBy of summarize).
# ---------------------------------------------------------------------------
def _map_summarize_kwargs(ds, class_value, var_value, out_value):
    try:
        params = list(inspect.signature(summarize).parameters.values())  # noqa: F405
    except (TypeError, ValueError):
        return None
    named = [
        p
        for p in params
        if p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    ]
    kwargs, taken = {}, set()
    for i, p in enumerate(named):
        n = p.name.lower().strip("_")
        if "class" not in taken and (
            "class" in n or "group" in n or n in ("by", "bys", "byvar", "byvars", "cls")
        ):
            kwargs[p.name] = [class_value] if isinstance(p.default, (list, tuple)) else class_value
            taken.add("class")
        elif "out" not in taken and ("out" in n or "target" in n):
            kwargs[p.name] = out_value
            taken.add("out")
        elif "var" not in taken and (
            "var" in n or "analysis" in n or "measure" in n or n in ("v", "value", "col")
        ):
            kwargs[p.name] = [var_value] if isinstance(p.default, (list, tuple)) else var_value
            taken.add("var")
        elif "ds" not in taken and (
            i == 0 or "ds" in n or "data" in n or n in ("df", "input", "inp", "table", "tbl", "src")
        ):
            kwargs[p.name] = ds
            taken.add("ds")
    if len(taken) < 4:
        return None
    return kwargs


def _call_summarize(ds, class_value, var_value, out_value):
    attempts = []
    mapped = _map_summarize_kwargs(ds, class_value, var_value, out_value)
    if mapped is not None:
        attempts.append(((), mapped))
    # positional, same parameter order as the SAS macro: ds, class, var, out
    attempts.append(((ds, class_value, var_value, out_value), {}))
    attempts.append(((ds, [class_value], var_value, out_value), {}))
    last_err = None
    for args, kwargs in attempts:
        try:
            return summarize(*args, **kwargs)  # noqa: F405
        except TypeError as err:  # signature did not match, try the next form
            last_err = err
    raise last_err


# ---------------------------------------------------------------------------
# %macro run_period(pid)
# ---------------------------------------------------------------------------
def run_period(pid):
    # data work.pay_&pid; set out.delinquency; where period_id = "&pid"; run;
    pay = spark.read.parquet(f"{OUT}/delinquency").filter(
        F.col("period_id").cast("string") == str(pid)
    )

    target = f"{OUT}/period_summary_{pid}"

    # %summarize(ds=work.pay_&pid, class=delinquent_flag, var=payment_amount,
    #            out=out.period_summary_&pid);
    for out_arg in (target, f"period_summary_{pid}", f"out.period_summary_{pid}"):
        res = _call_summarize(pay, "delinquent_flag", "payment_amount", out_arg)
        if os.path.isdir(target):
            return
        if isinstance(res, DataFrame):
            res.write.mode("overwrite").parquet(target)
            return
        nested = os.path.join(OUT, out_arg)  # macro may add the library path itself
        if os.path.isdir(nested) and os.path.abspath(nested) != os.path.abspath(target):
            spark.read.parquet(nested).write.mode("overwrite").parquet(target)
            return


# ---------------------------------------------------------------------------
# %macro period_driver
# ---------------------------------------------------------------------------
def period_driver():
    # %do i = 1 %to %sysfunc(countw(&period_list, %str( )));
    for i in range(1, len(period_list) + 1):
        pid = period_list[i - 1]  # %let pid = %scan(&period_list, &i, %str( ));
        run_period(pid)


period_driver()

spark.stop()
