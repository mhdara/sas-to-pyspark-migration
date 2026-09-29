import os, sys
import inspect
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("07_portfolio_report").getOrCreate()

# %include "&root/programs/06_macro_library.sas"  ->  macro library (%summarize, %flag_high)
from p06_macro_library import *

# ---------------------------------------------------------------------------
# LIBRARY PATHS (relative to the project root)
# ---------------------------------------------------------------------------
CTRL = "data/parquet/ctrl"
STG  = "data/parquet/stg"
OUT  = "data/parquet/py_out/llm_remote/run1"

# ---------------------------------------------------------------------------
# proc sql noprint;
#   select param_value into :high_principal trimmed
#   from ctrl.macro_parameters where param_name = 'HIGH_PRINCIPAL';
# quit;
# ---------------------------------------------------------------------------
_row = (spark.read.parquet(f"{CTRL}/macro_parameters.parquet")
        .filter(F.col("param_name") == "HIGH_PRINCIPAL")
        .select(F.trim(F.col("param_value").cast("string")).alias("param_value"))
        .first())

# the parameter is stored as text ("5000000"); SAS substitutes it into a numeric
# comparison inside %flag_high, so it must become a number here.
high_principal_txt = _row[0] if _row is not None else None
high_principal = float(high_principal_txt) if high_principal_txt is not None else None
if high_principal is None:
    # SAS would leave &high_principal unresolved -> program would fail; keep it visible.
    raise ValueError("HIGH_PRINCIPAL not found in ctrl.macro_parameters")  # TODO MANUAL


def _invoke(fn, sas_ordered_args, out_path):
    """Call a converted macro from p06_macro_library with the SAS macro parameter
    order (ds, class, var, out) / (ds, var, threshold, out).  The converted function
    may use a renamed 'class' parameter (class_/cls), therefore the arguments are
    bound positionally.

    IMPORTANT: the macro library resolves SAS two-level names (libref.table) to
    physical paths itself, so the dataset arguments must be passed as the SAS
    names ('out.loan_enriched'), never as already-resolved paths (that produced
    a doubled path and PATH_NOT_FOUND).

    If the function returns a DataFrame instead of writing it, the result is
    persisted here so the SAS output table always exists."""
    try:
        params = [p for p in inspect.signature(fn).parameters.values()
                  if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)]
        res = fn(**{p.name: v for p, v in zip(params, sas_ordered_args)})
    except TypeError:
        res = fn(*sas_ordered_args)
    if res is not None and hasattr(res, "write"):
        res.write.mode("overwrite").parquet(out_path)
    return res


# ---------------------------------------------------------------------------
# %summarize(ds=out.loan_enriched, class=segment_name, var=principal,
#            out=out.portfolio_by_segment);
# ---------------------------------------------------------------------------
_invoke(summarize,
        ["out.loan_enriched", "segment_name", "principal", "out.portfolio_by_segment"],
        f"{OUT}/portfolio_by_segment")

# ---------------------------------------------------------------------------
# %flag_high(ds=out.portfolio_by_segment, var=total, threshold=&high_principal,
#            out=out.portfolio_report);
#   NOTE: missing/NULL handling of the comparison (SAS: missing < any number)
#         lives inside the converted %flag_high macro in p06_macro_library.
# ---------------------------------------------------------------------------
_invoke(flag_high,
        ["out.portfolio_by_segment", "total", high_principal, "out.portfolio_report"],
        f"{OUT}/portfolio_report")

spark.stop()
