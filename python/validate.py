"""validate.py - compare a converted program's output tables with the SAS results (the expected values).

For each `out` table a SAS program writes (from the analyzer), the SAS table in sas/outputs/ is compared
with the table the converted Python program wrote, using the same checks as the data migration
(migrate_data.reconcile), after the same preparation on both sides:
  lower-case column names, numbers rounded to 6 decimals (tolerance 1e-6), rows sorted.
Then named checks for the planted migration traps (data_specs.md) run on the Python output.

Run:  python python/validate.py --method llm_remote [--run 1] [program_id ...]
Out:  outputs/conversion/validation_<method>_run<k>.csv
Used by convert.py after every attempt: validate_program(pid, out_dir).
"""

import argparse
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from migrate_data import read_sas, reconcile

ROOT = Path(__file__).resolve().parents[1]
SAS_OUT = ROOT / "sas" / "outputs"
DECIMALS = 6  # numeric tolerance 1e-6
# model outputs: SAS writes extra bookkeeping columns (_MODEL_, _TYPE_, ...); compare the estimates only
ONLY_COLUMNS = {"reg_est": ["intercept", "principal", "interest_rate", "term_months"]}
# forecast outputs: compare what is reproducible; SAS's 95% limits and fit statistics are not
ONLY_ROWS = {
    "fc": ("_type_", {"ACTUAL", "FORECAST"}),
    "fc_est": ("_type_", {"S1", "S2", "CONSTANT", "LINEAR"}),
}


def py_out_dir(method: str, run: int) -> Path:
    """Folder where the converted programs of one method and run write their tables."""
    return ROOT / "data" / "parquet" / "py_out" / method / f"run{run}"


def outputs_of(pid: str) -> list[str]:
    """The `out` tables a SAS program writes, according to the analyzer (resolved names only)."""
    deps = pd.read_csv(ROOT / "outputs" / "program_dependencies.csv")
    d = deps[(deps.program_id == pid) & (deps.relationship == "WRITES") & (deps.object_type == "TABLE")]
    return sorted({o.split(".", 1)[1] for o in d.object if o.startswith("out.") and "&" not in o})


def prepare(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Make SAS and Python tables comparable: names, selected columns, rounding, row order."""
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    if table in ONLY_COLUMNS:
        df = df[[c for c in ONLY_COLUMNS[table] if c in df.columns]]
    if table in ONLY_ROWS and ONLY_ROWS[table][0] in df.columns:
        column, keep = ONLY_ROWS[table]
        df = df[df[column].astype(str).str.upper().isin(keep)]
    for c in df.columns:
        if pd.api.types.is_bool_dtype(df[c]):
            df[c] = df[c].astype(float)
        if pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c].astype(float).round(DECIMALS)
    # Spark does not keep SAS's row order: sort both sides by every column (as text, nulls last)
    key = df.astype(str).where(df.notna(), "~")
    return df.loc[key.sort_values(list(key.columns)).index].reset_index(drop=True)


def read_python_table(folder: Path) -> pd.DataFrame:
    """Read a table written by Spark (a folder of Parquet parts) or a single Parquet file."""
    return pq.read_table(folder).to_pandas()


def planted_checks(pid: str, out_dir: Path) -> list[dict]:
    """Named checks for the migration traps planted in the data, on the Python output."""

    def table(name):
        path = out_dir / name
        return read_python_table(path) if path.exists() else None

    def check(name, passed, detail):
        return {
            "program_id": pid,
            "table": "",
            "column": "",
            "check": f"planted_{name}",
            "expected_value": "",
            "actual_value": detail,
            "passed": int(bool(passed)),
            "compared": "python output",
        }

    rows = []
    if pid == "01_customer_clean" and (t := table("customers_clean")) is not None:
        missing = t[t.annual_income.isna()]
        rows.append(
            check("province_upper_case", (t.province == "QC").all(), f"{(t.province != 'QC').sum()} not QC")
        )
        rows.append(
            check(
                "missing_income_is_low",
                len(missing) == 15 and (missing.income_band == "LOW").all(),
                f"{len(missing)} missing incomes, bands {sorted(missing.income_band.unique())}",
            )
        )
    if pid == "02_loan_enrichment" and (t := table("loan_enriched")) is not None:
        rows.append(
            check(
                "orphan_loan_dropped",
                len(t) == 799 and 999 not in set(t.customer_id),
                f"{len(t)} loans, customer 999 present: {999 in set(t.customer_id)}",
            )
        )
    if pid == "03_risk_format" and (t := table("customer_risk")) is not None:
        missing = t[t.annual_income.isna()]
        rows.append(
            check(
                "missing_income_is_unknown",
                (missing.income_band_fmt == "UNKNOWN").all() and len(missing) == 15,
                f"bands of missing incomes: {sorted(missing.income_band_fmt.unique())}",
            )
        )
    if pid == "04_payment_delinquency" and (t := table("delinquency")) is not None:
        dup = int(t.duplicated(["loan_id", "payment_date"]).sum())
        rows.append(
            check(
                "duplicate_payment_removed", dup == 0 and len(t) == 21411, f"{len(t)} rows, {dup} duplicates"
            )
        )
    if pid == "05_card_fraud_summary" and (t := table("card_summary")) is not None:
        source = pd.read_parquet(ROOT / "data" / "parquet" / "stg" / "card_transactions.parquet")
        rows.append(
            check(
                "refunds_and_zeros_in_totals",
                abs(t.total_amount.sum() - source.amount.sum()) < 1e-6,
                f"total {t.total_amount.sum():.2f} vs source {source.amount.sum():.2f}",
            )
        )
    if pid == "08_period_driver":
        found = sorted(p.name for p in out_dir.glob("period_summary_*"))
        rows.append(check("one_table_per_active_period", len(found) == 4, ", ".join(found) or "none"))
    return rows


def validate_program(pid: str, out_dir: Path) -> list[dict]:
    """Compare every SAS output table of a program with the Python one; one row per check."""
    rows = []
    for table in outputs_of(pid):
        sas_file, py_path = SAS_OUT / f"{table}.sas7bdat", out_dir / table
        base = {"program_id": pid, "table": f"out.{table}", "column": "", "compared": "sas vs python"}
        if not py_path.exists():
            rows.append(
                base
                | {
                    "check": "python_output",
                    "expected_value": "written",
                    "actual_value": "not written",
                    "passed": 0,
                }
            )
            continue
        sas_df = prepare(read_sas(sas_file)[0], table)
        py_df = prepare(read_python_table(py_path), table)
        rows += [{"program_id": pid, **r} for r in reconcile(f"out.{table}", sas_df, py_df, "sas", "python")]
    return rows + planted_checks(pid, out_dir)


def main():
    """Validate the converted programs of one method and run, and save the checks."""
    parser = argparse.ArgumentParser(description="compare converted programs' outputs with SAS")
    parser.add_argument("--method", required=True)
    parser.add_argument("--run", type=int, default=1)
    parser.add_argument("programs", nargs="*")
    args = parser.parse_args()
    inv = pd.read_csv(ROOT / "outputs" / "migration_inventory.csv")
    pids = args.programs or [p for p in inv.program_id if p != "00_load_raw"]
    out_dir = py_out_dir(args.method, args.run)
    result = pd.DataFrame([r for pid in pids for r in validate_program(pid, out_dir)])
    target = ROOT / "outputs" / "conversion" / f"validation_{args.method}_run{args.run}.csv"
    target.parent.mkdir(exist_ok=True)
    result.to_csv(target, index=False)
    print(result.groupby("program_id").passed.agg(checks="size", passed="sum").to_string())


if __name__ == "__main__":
    main()
