"""lineage_bi.py - build the Power BI tables: one clear grain per table, CSV in outputs/bi/.

Two stars share nothing but the date table:
  migration star  DimProgram, DimMethod, DimTable -> FactClassification, FactConversionAttempt,
                  FactConversionStatus, FactValidationCheck, FactReconciliationCheck, FactLineageEdge,
                  FactLineagePath
  bank star       DimCustomer, DimSegment, DimLoan, DimDate -> FactLoanPayment, FactCardTransaction
plus small stand-alone tables (forecast, showcase names, summaries).

Bank data comes from the migrated Parquet tables (the Databricks side) and, for derived columns
(income bands, late-payment flags), from the SAS reference results.
Run:  python python/lineage_bi.py            Out: outputs/bi/*.csv (committed; Windows gets them by git pull)
"""

import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
from migrate_data import read_sas

OUT, BI = ROOT / "outputs", ROOT / "outputs" / "bi"
PARQ, SAS_OUT = ROOT / "data" / "parquet", ROOT / "sas" / "outputs"
METHODS = [  # method_id, kind, description, deterministic
    ("rules_v1.1", "classification", "Guide's rules, tuned on 3 programs (protocol)", 1),
    ("rules_v2", "classification", "Broader rules, written after seeing v1.1 (optimistic)", 1),
    ("llm_claude_opus_5", "classification", "Claude Opus 5, remote API", 0),
    ("llm_qwen2.5_coder_14b", "classification", "qwen2.5-coder 14B, local (Ollama)", 0),
    ("rule_based", "conversion", "Template translator for a SAS subset, no LLM", 1),
    ("llm_remote", "conversion", "Claude Opus 5 + targeted rules + one repair", 0),
    ("llm_local", "conversion", "qwen2.5-coder 14B + targeted rules + one repair", 0),
    (
        "lakebridge_sas_generic",
        "conversion",
        "Databricks Lakebridge Switch, built-in SAS prompt (headline)",
        0,
    ),
    (
        "lakebridge_sas_generic_params",
        "conversion",
        "Lakebridge Switch headline + root parameter (sensitivity)",
        0,
    ),
    ("lakebridge_sas", "conversion", "Lakebridge Switch, SAS prompt, default SQL preprocessing (pitfall)", 0),
    ("lakebridge_unknown_etl", "conversion", "Lakebridge Switch, unknown_etl route from the docs", 0),
    (
        "lakebridge_sas_custom",
        "conversion",
        "Lakebridge Switch with a custom prompt: built-in SAS prompt corrected + the project's rules",
        0,
    ),
]


def save(df: pd.DataFrame, name: str) -> None:
    """Write one table as UTF-8 CSV; line breaks inside text are removed (they break Power BI imports)."""
    df = df.copy()
    for c in df.select_dtypes(include=["object", "string"]).columns:
        df[c] = df[c].map(lambda v: re.sub(r"[\r\n]+", " ", v) if isinstance(v, str) else v)
    df.to_csv(BI / f"{name}.csv", index=False, encoding="utf-8")
    print(f"  {name:26s} {len(df):>6} rows")


def concat_runs(kind: str) -> pd.DataFrame:
    """All outputs/conversion/<kind>_<method>_run<k>.csv files in one table (method and run as columns)."""
    frames = []
    for path in sorted((OUT / "conversion").glob(f"{kind}_*_run*.csv")):
        method, run = re.fullmatch(rf"{kind}_(.+)_run(\d+)", path.stem).groups()
        frames.append(pd.read_csv(path).assign(method=method, run=int(run)))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def migration_star() -> None:
    """Programs, methods, tables and every result of phases 6-12."""
    inv = pd.read_csv(OUT / "migration_inventory.csv")
    gold = pd.read_csv(ROOT / "rules" / "gold_labels.csv").add_prefix("gold_")
    save(inv.merge(gold, left_on="program_id", right_on="gold_program_id").drop(columns="gold_program_id"),
         "DimProgram")  # fmt: skip
    save(pd.DataFrame(METHODS, columns=["method_id", "kind", "description", "deterministic"]), "DimMethod")

    nodes, edges = pd.read_csv(OUT / "lineage_nodes.csv"), pd.read_csv(OUT / "lineage_edges.csv")
    tables = nodes[nodes.node_type == "TABLE"].rename(columns={"node_id": "table_id"})[
        ["table_id", "library"]
    ]
    rec = pd.read_csv(OUT / "reconciliation_results.csv")
    rec["table_id"] = rec.table.str.split(" ").str[0]
    rec["scenario"] = rec.table.str.partition(" ")[2].replace("", "migration")
    rows = (
        rec[(rec.check == "row_count") & (rec.scenario == "migration")].set_index("table_id").expected_value
    )
    save(tables.assign(row_count=tables.table_id.map(rows)), "DimTable")
    save(rec.drop(columns="table"), "FactReconciliationCheck")  # 1 row = 1 table x 1 check

    save(pd.read_csv(OUT / "classification_comparison.csv"), "FactClassification")  # program x field x method
    for kind, name in (("attempts", "FactConversionAttempt"), ("status", "FactConversionStatus")):
        df = concat_runs(kind)
        save(df[df.program_id != "00_load_raw"] if len(df) else df, name)
    val = concat_runs("validation")  # 1 row = 1 program x table x check x method x run
    if len(val):
        val["planted"] = val.check.str.startswith("planted_").astype(int)
        save(val.rename(columns={"table": "table_id"}), "FactValidationCheck")

    types = nodes.set_index("node_id").node_type
    save(edges.assign(source_type=edges.source_id.map(types), target_type=edges.target_id.map(types), weight=1),
         "FactLineageEdge")  # fmt: skip
    if (OUT / "lineage_paths.csv").exists():  # back-trace: every output table with everything upstream of it
        save(pd.read_csv(OUT / "lineage_paths.csv"), "FactLineagePath")
    for src, name in (
        ("forecast_parity_detail.csv", "FactForecast"),
        ("showcase_names.csv", "ShowcaseNames"),
        ("comparison_summary.csv", "ClassificationSummary"),
        ("conversion/summary_by_method.csv", "ConversionSummary"),
    ):
        if (OUT / src).exists():
            save(pd.read_csv(OUT / src), name)


def bank_star() -> None:
    """The data the SAS programs work on: customers, segments, loans, payments, cards, dates."""
    customers = pd.read_parquet(PARQ / "stg" / "customers.parquet")
    bands = read_sas(SAS_OUT / "customer_risk.sas7bdat")[0][["customer_id", "income_band_fmt"]]
    save(
        customers.assign(province=customers.province.str.upper()).merge(  # cleaned as in program 01
            bands.rename(columns={"income_band_fmt": "income_band"}), on="customer_id", how="left"
        ),
        "DimCustomer",
    )  # income band from program 03: missing income -> UNKNOWN
    save(pd.read_parquet(PARQ / "ctrl" / "client_segments.parquet"), "DimSegment")
    save(pd.read_parquet(PARQ / "stg" / "loans.parquet"), "DimLoan")  # the orphan loan (customer 999) stays
    payments = read_sas(SAS_OUT / "delinquency.sas7bdat")[0]  # deduplicated + late flag (program 04)
    save(payments.drop(columns=["cum_paid", "max_dpd"], errors="ignore"), "FactLoanPayment")
    save(pd.read_parquet(PARQ / "stg" / "card_transactions.parquet"), "FactCardTransaction")
    dates = pd.DataFrame({"date": pd.date_range("2022-01-01", "2026-12-31", freq="D")})
    save(
        dates.assign(
            year=dates.date.dt.year,
            month=dates.date.dt.month,
            month_start=dates.date.dt.to_period("M").dt.start_time,
            month_label=dates.date.dt.strftime("%Y-%m"),
        ),
        "DimDate",
    )


def main():
    """Rebuild every Power BI table in outputs/bi/."""
    BI.mkdir(parents=True, exist_ok=True)
    print("migration star:")
    migration_star()
    print("bank star:")
    bank_star()


if __name__ == "__main__":
    main()
