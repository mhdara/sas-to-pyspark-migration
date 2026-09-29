"""migrate_data.py - DATA migration: SAS tables (.sas7bdat) -> Parquet + metadata sidecar + reconciliation.

Run:  python python/migrate_data.py
In:   sas/data/stg/*.sas7bdat, sas/data/ctrl/*.sas7bdat   (downloaded from SAS OnDemand)
      data/raw/*.csv                                      (the source files, for the source check)
      sas/data/v1/customers.sas7bdat                      (optional: the truncated v1 load, $12)
Out:  data/parquet/<lib>/<table>.parquet + <table>.metadata.json
      outputs/reconciliation_results.csv, outputs/showcase_names.csv
Wording: "no discrepancies detected across N checks on the test dataset" - never "zero data loss".
Every check says whether it is EXPECTED to pass: the truncation evidence and the negative test are
designed to fail. The run fails (exit code 1) only when a check does not do what is expected.
"""

import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
SAS_DATA, PARQ, RAW, OUT = (
    ROOT / "sas" / "data",
    ROOT / "data" / "parquet",
    ROOT / "data" / "raw",
    ROOT / "outputs",
)
TOL = 1e-9
SHOWCASE = ["Émilie Tremblay", "François Bélanger", "Chloé Gagnon", "Marie-Ève Beauchemin-Laflamme"]


# ---------------------------------------------------------------- reading SAS
def read_sas(path, encoding=None, bytesafe=False):
    """Read a SAS table: lower-case column names, trailing blanks removed, empty text = missing.

    bytesafe=True reads a table whose text may hold invalid UTF-8 (a character cut in half by a too
    short column, as in the v1 load): every byte is read as latin1, then decoded as UTF-8 with
    broken characters shown as U+FFFD instead of crashing.
    """
    import pyreadstat

    df, meta = pyreadstat.read_sas7bdat(str(path), encoding="latin1" if bytesafe else encoding)
    if bytesafe:
        for c in df.columns:
            if df[c].map(lambda v: isinstance(v, str)).any():
                df[c] = df[c].map(
                    lambda v: (
                        v.encode("latin1").decode("utf-8", errors="replace") if isinstance(v, str) else v
                    )
                )
    df.columns = [c.lower() for c in df.columns]
    for c in df.columns:  # SAS pads text with blanks: remove TRAILING only
        if df[c].map(lambda v: isinstance(v, str)).any():
            df[c] = df[c].map(lambda v: v.rstrip(" ") if isinstance(v, str) else v).replace("", None)
    return df, meta


def arrow_schema(df):
    fields = []
    for c in df.columns:
        s = df[c].dropna()
        first = s.iloc[0] if len(s) else None
        if isinstance(first, (dt.datetime, pd.Timestamp)):
            fields.append(pa.field(c, pa.timestamp("us")))
        elif isinstance(first, dt.date):
            fields.append(pa.field(c, pa.date32()))
        elif isinstance(first, str) or pd.api.types.is_string_dtype(df[c]):
            fields.append(pa.field(c, pa.string()))
        else:
            fields.append(pa.field(c, pa.float64()))  # SAS numbers are 8-byte floats
    return pa.schema(fields)


def write_parquet(df, meta, lib, table):
    (PARQ / lib).mkdir(parents=True, exist_ok=True)
    target = PARQ / lib / f"{table}.parquet"
    pq.write_table(pa.Table.from_pandas(df, schema=arrow_schema(df), preserve_index=False), target)
    sidecar = {
        "source": f"{lib}.{table}",
        "file_encoding": meta.file_encoding,
        "number_rows": meta.number_rows,
        "number_columns": meta.number_columns,
        "column_names": list(df.columns),
        "column_labels": meta.column_labels,
        "sas_types": meta.readstat_variable_types,
        "sas_formats": meta.original_variable_types,
        "storage_widths": meta.variable_storage_width,
        "converted_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
    }
    (PARQ / lib / f"{table}.metadata.json").write_text(
        json.dumps(sidecar, indent=1, ensure_ascii=False, default=str), encoding="utf-8"
    )
    return target


# ---------------------------------------------------------------- reconciliation
def canon(v):
    if isinstance(v, (bool, np.bool_)):
        v = float(v)
    if v is None or (isinstance(v, float) and np.isnan(v)) or v is pd.NaT:
        return "<NULL>"
    if isinstance(v, (dt.date, dt.datetime, pd.Timestamp)):
        return pd.Timestamp(v).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, (int, float, np.integer, np.floating)) and not isinstance(v, bool):
        return f"{float(v):.9f}"
    return str(v)


def row_hashes(df):
    cols = sorted(df.columns)
    return sorted(
        hashlib.sha256("|".join(canon(v) for v in row).encode("utf-8")).hexdigest()
        for row in df[cols].itertuples(index=False, name=None)
    )


def reconcile(name, a, b, a_label="sas", b_label="parquet"):
    """Compare two DataFrames (a = expected, b = migrated). Returns a list of check rows."""
    res = []

    def add(check, va, vb, passed, column=""):
        res.append(
            {
                "table": name,
                "column": column,
                "check": check,
                "expected_value": str(va),
                "actual_value": str(vb),
                "passed": int(bool(passed)),
                "compared": f"{a_label} vs {b_label}",
            }
        )

    add("row_count", len(a), len(b), len(a) == len(b))
    add("column_count", a.shape[1], b.shape[1], a.shape[1] == b.shape[1])
    add("column_names", sorted(a.columns), sorted(b.columns), sorted(a.columns) == sorted(b.columns))
    for c in [c for c in a.columns if c in b.columns]:
        x, y = a[c], b[c]
        add("null_count", int(x.isna().sum()), int(y.isna().sum()), x.isna().sum() == y.isna().sum(), c)
        if pd.api.types.is_numeric_dtype(x) and pd.api.types.is_numeric_dtype(y):
            add("sum", x.sum(), y.sum(), abs(x.sum() - y.sum()) <= TOL * max(1, abs(x.sum())), c)
            add(
                "min_max",
                (x.min(), x.max()),
                (y.min(), y.max()),
                (x.min(), x.max()) == (y.min(), y.max()),
                c,
            )
        elif x.map(lambda v: isinstance(v, str)).any():
            lx, ly = x.dropna().map(len).max(), y.dropna().map(len).max()
            bx = x.dropna().map(lambda s: len(str(s).encode("utf-8"))).max()
            by = y.dropna().map(lambda s: len(str(s).encode("utf-8"))).max()
            add("max_length_chars", lx, ly, lx == ly, c)
            add("max_length_utf8_bytes", bx, by, bx == by, c)
            add("distinct_count", x.nunique(), y.nunique(), x.nunique() == y.nunique(), c)
            mism = (
                int((x.fillna("<NULL>").astype(str).values != y.fillna("<NULL>").astype(str).values).sum())
                if len(x) == len(y)
                else -1
            )
            add("exact_text_values", 0, mism, mism == 0, c)
    ha, hb = row_hashes(a), row_hashes(b)
    only_a, only_b = len(set(ha) - set(hb)), len(set(hb) - set(ha))
    add("row_hash_sha256", f"{only_a} rows only in {a_label}", f"{only_b} rows only in {b_label}", ha == hb)
    return res


# ---------------------------------------------------------------- main
def main():
    OUT.mkdir(exist_ok=True)
    results, showcase = [], []
    for lib in ("ctrl", "stg"):
        for f in sorted((SAS_DATA / lib).glob("*.sas7bdat")):
            table = f.stem.lower()
            sas_df, meta = read_sas(f)
            target = write_parquet(sas_df, meta, lib, table)
            back = pq.read_table(target).to_pandas()
            results += reconcile(f"{lib}.{table}", sas_df, back)
            raw = RAW / (f"ctrl_{table}.csv" if lib == "ctrl" else f"{table}.csv")
            if raw.exists():  # source file vs SAS table (catches load truncation)
                src = pd.read_csv(raw, dtype=str, keep_default_na=False).replace("", None)
                txt = [
                    c
                    for c in src.columns
                    if c in sas_df.columns and sas_df[c].map(lambda v: isinstance(v, str)).any()
                ]
                results += [
                    r | {"check": "source_" + r["check"]}
                    for r in reconcile(
                        f"{lib}.{table}", src[txt], sas_df[txt].astype(object), "source_csv", "sas"
                    )
                    if r["check"] in ("exact_text_values", "max_length_chars")
                ]
            print(f"{lib}.{table:22s} rows={len(sas_df):>6}  encoding={meta.file_encoding}")
            if table == "customers":
                for n in SHOWCASE:
                    hit = back[back.customer_name == n]
                    showcase.append(
                        {
                            "source_csv_value": n,
                            "sas_value": n if (sas_df.customer_name == n).any() else "(not found)",
                            "parquet_value": hit.customer_name.iloc[0] if len(hit) else "(not found)",
                            "utf8_bytes": len(n.encode("utf-8")),
                        }
                    )
                # NEGATIVE TEST: wrong encoding must be detected (the check must FAIL)
                bad, _ = read_sas(f, encoding="latin1")
                neg = reconcile(
                    "stg.customers (read as latin1)", sas_df, bad, "correct_read", "wrong_encoding"
                )
                detected = any(r["passed"] == 0 for r in neg if r["column"] == "customer_name")
                results.append(
                    {
                        "table": "stg.customers",
                        "column": "customer_name",
                        "check": "negative_test_wrong_encoding",
                        "expected_value": "corruption detected",
                        "actual_value": "detected" if detected else "NOT detected",
                        "passed": int(detected),
                        "compared": "correct_read vs latin1_read",
                    }
                )
    v1 = SAS_DATA / "v1" / "customers.sas7bdat"  # truncation evidence (expected to FAIL)
    if v1.exists():
        v1_df, _ = read_sas(v1, bytesafe=True)  # a plain UTF-8 read crashes on the half characters
        src = pd.read_csv(RAW / "customers.csv", dtype=str)
        for r in reconcile(
            "stg.customers (v1 load, $12)",
            src[["customer_name"]],
            v1_df[["customer_name"]].astype(object),
            "source_csv",
            "sas_v1",
        ):
            if r["check"] in ("exact_text_values", "max_length_chars"):
                results.append(r | {"check": "truncation_demo_" + r["check"]})
        broken = int(v1_df.customer_name.str.contains("\ufffd", na=False).sum())
        results.append(
            {
                "table": "stg.customers (v1 load, $12)",
                "column": "customer_name",
                "check": "truncation_demo_invalid_utf8",
                "expected_value": "0 names with a broken character",
                "actual_value": f"{broken} names cut inside a 2-byte character",
                "passed": int(broken == 0),
                "compared": "source_csv vs sas_v1",
            }
        )
    r = pd.DataFrame(results)
    # evidence checks are designed to fail; the negative test passes when it DETECTS the corruption
    r["expected_to_pass"] = (~r.check.str.startswith("truncation_demo_")).astype(int)
    r["as_expected"] = (r.passed == r.expected_to_pass).astype(int)
    r.to_csv(OUT / "reconciliation_results.csv", index=False)
    pd.DataFrame(showcase).to_csv(OUT / "showcase_names.csv", index=False)
    evidence = r[r.expected_to_pass == 0]
    unexpected = r[r.as_expected == 0]
    print(
        f"\n{len(r)} checks: {len(r) - len(evidence)} migration checks, {len(evidence)} truncation-evidence "
        f"checks (designed to fail: {int((evidence.passed == 0).sum())} failed)"
    )
    print(f"unexpected results: {len(unexpected)}")
    if len(unexpected):
        print(
            unexpected[["table", "column", "check", "expected_value", "actual_value"]].to_string(index=False)
        )
    return 1 if len(unexpected) else 0


if __name__ == "__main__":
    sys.exit(main())
