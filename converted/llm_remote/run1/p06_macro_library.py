import os, sys
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("06_macro_library").getOrCreate()

# ----------------------------------------------------------------------------------------------
# 06_macro_library.sas - shared macro library. Creates no data by itself.
# Converted to a Python module: programs 07 and 08 do "from p06_macro_library import *"
# and call summarize(...) / flag_high(...).
# ----------------------------------------------------------------------------------------------

LIBS = {
    "ctrl": "data/parquet/ctrl",
    "stg": "data/parquet/stg",
    "out": "data/parquet/py_out/llm_remote/run1",
}


def _resolve_path(name):
    """Map a SAS style <lib>.<table> (or a bare table name, assumed to live in OUT) to a path."""
    if "." in name:
        lib, tbl = name.split(".", 1)
        lib = lib.strip().lower()
        tbl = tbl.strip().lower()
    else:
        lib, tbl = "out", name.strip().lower()
    if lib in ("ctrl", "stg"):
        return f"{LIBS[lib]}/{tbl}.parquet"
    # tables written by other converted programs (work/out) live under the out path
    return f"{LIBS['out']}/{tbl}"


def _load(ds):
    """Accept either a DataFrame (preferred, in-memory WORK dataset) or a SAS table name."""
    if ds is None:
        raise ValueError("ds= is required")
    if hasattr(ds, "columns") and hasattr(ds, "select"):
        return ds
    return spark.read.parquet(_resolve_path(str(ds)))


def _save(df, out):
    """Write the result like SAS writes &out; a name without a library goes to the OUT path."""
    if out is None:
        return df
    if hasattr(out, "columns") and hasattr(out, "select"):
        # cannot write to a DataFrame; just return the result
        return df
    name = str(out)
    if "." in name:
        lib, tbl = name.split(".", 1)
        lib = lib.strip().lower()
        tbl = tbl.strip().lower()
    else:
        lib, tbl = "out", name.strip().lower()
    if lib in ("ctrl", "stg"):
        # SAS could write there, but converted programs only ever write to OUT
        path = f"{LIBS[lib]}/{tbl}"  # TODO MANUAL: writing back to a source library
    else:
        path = f"{LIBS['out']}/{tbl}"
    df.write.mode("overwrite").parquet(path)
    return df


def _as_list(cols):
    if cols is None:
        return []
    if isinstance(cols, (list, tuple)):
        return [str(c).strip().lower() for c in cols if str(c).strip() != ""]
    return [c.strip().lower() for c in str(cols).replace(",", " ").split() if c.strip() != ""]


# %macro summarize(ds=, class=, var=, out=);
#   proc means data=&ds noprint nway; class &class; var &var;
#     output out=&out(drop=_type_ _freq_) n=n sum=total mean=avg;
#   run;
# %mend summarize;
def summarize(ds=None, class_=None, var=None, out=None, **kwargs):
    # "class" is a Python keyword: accept both class_= and class= (via **kwargs)
    if class_ is None:
        class_ = kwargs.get("class", kwargs.get("clazz", kwargs.get("cls")))
    df = _load(ds)
    class_cols = _as_list(class_)
    var_col = str(var).strip().lower()

    # PROC MEANS NWAY + CLASS -> groupBy(class); N counts NON-MISSING values of &var.
    # SUM / MEAN ignore missing values (null when every value in the group is missing).
    res = (
        df.groupBy(*[F.col(c) for c in class_cols])
        .agg(
            F.count(F.col(var_col)).cast("long").alias("n"),
            F.sum(F.col(var_col)).alias("total"),
            F.avg(F.col(var_col)).alias("avg"),
        )
        .select(*class_cols, "n", "total", "avg")  # _type_ / _freq_ dropped as in SAS
        .orderBy(*[F.col(c).asc_nulls_first() for c in class_cols])  # PROC MEANS orders by CLASS
    )
    return _save(res, out)


# %macro flag_high(ds=, var=, threshold=, out=);
#   data &out; set &ds; high_flag = (&var > &threshold); run;
# %mend flag_high;
def flag_high(ds=None, var=None, threshold=None, out=None, **kwargs):
    df = _load(ds)
    var_col = str(var).strip().lower()
    thr = float(threshold)

    # missing values give 0 in SAS (missing sorts below any number)
    high = (
        F.when(F.col(var_col).isNull(), F.lit(0))
        .otherwise((F.col(var_col) > F.lit(thr)).cast("int"))
        .cast("int")
    )
    res = df.withColumn("high_flag", high)
    return _save(res, out)


if __name__ == "__main__":
    # The SAS program only defines macros; it reads and writes nothing on its own.
    print("06_macro_library: macro library module, nothing to run standalone.")
    spark.stop()
