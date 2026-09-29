"""compare.py - every classification method vs the gold labels, and vs each other.

Methods: rules v1.1 (protocol), rules v2 (written after seeing v1.1: optimistic on these programs),
and one LLM per outputs/llm_analysis_<model>.csv file.
Run:  python python/compare.py
Out:  outputs/classification_comparison.csv  one row per program x field x method (label, correct?)
      outputs/comparison_summary.csv         accuracy per field x method
      outputs/method_agreement.csv           agreement % and Cohen's kappa per field x pair of methods
"""

import itertools
from pathlib import Path

import pandas as pd
from sklearn.metrics import cohen_kappa_score

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs"
FIELDS = ["business_category", "technical_category", "complexity"]
OPTIMISTIC = {"rules_v2"}  # written after seeing v1.1's results on these programs


def methods() -> dict[str, pd.DataFrame]:
    """Every method's labels, indexed by program_id: the two rule versions and each LLM output file."""
    found = {
        "rules_v1.1": pd.read_csv(OUT / "rule_analysis.csv"),
        "rules_v2": pd.read_csv(OUT / "rule_analysis_v2.csv"),
    }
    for path in sorted(OUT.glob("llm_analysis_*.csv")):
        found["llm_" + path.stem.removeprefix("llm_analysis_")] = pd.read_csv(path)
    return {name: df.set_index("program_id")[FIELDS] for name, df in found.items()}


def kappa(a: pd.Series, b: pd.Series) -> float:
    """Cohen's kappa: agreement corrected for chance; undefined (NaN) when both use a single label."""
    if a.nunique() == 1 and b.nunique() == 1:
        return float("nan")
    return round(cohen_kappa_score(a, b), 3)


def main():
    gold = pd.read_csv(ROOT / "rules" / "gold_labels.csv").set_index("program_id")
    labels = methods()
    rows = [
        {
            "program_id": pid,
            "field": field,
            "gold": gold.loc[pid, field],
            "method": name,
            "label": df.loc[pid, field],
            "correct": int(df.loc[pid, field] == gold.loc[pid, field]),
        }
        for name, df in labels.items()
        for pid in gold.index
        for field in FIELDS
    ]
    comparison = pd.DataFrame(rows)
    summary = (
        comparison.groupby(["field", "method"])
        .correct.agg(n="size", accuracy_pct=lambda c: round(100 * c.mean(), 1))
        .reset_index()
        .assign(optimistic=lambda s: s.method.isin(OPTIMISTIC).astype(int))
    )
    agreement = pd.DataFrame(
        [
            {
                "field": field,
                "method_a": a,
                "method_b": b,
                "agreement_pct": round(100 * (labels[a][field] == labels[b][field]).mean(), 1),
                "cohen_kappa": kappa(labels[a][field], labels[b][field]),
            }
            for field in FIELDS
            for a, b in itertools.combinations(labels, 2)
        ]
    )
    comparison.to_csv(OUT / "classification_comparison.csv", index=False)
    summary.to_csv(OUT / "comparison_summary.csv", index=False)
    agreement.to_csv(OUT / "method_agreement.csv", index=False)

    print("Accuracy vs gold (%), 12 case programs  (* = optimistic: written after seeing v1.1)")
    table = summary.pivot(index="method", columns="field", values="accuracy_pct")[FIELDS]
    table.index = [f"{m}{' *' if m in OPTIMISTIC else ''}" for m in table.index]
    print(table.to_string())
    print("\nAgreement between methods (% / kappa)")
    print(agreement.to_string(index=False))
    wrong = comparison[comparison.correct == 0].pivot(
        index=["program_id", "field", "gold"], columns="method", values="label"
    )
    print("\nMismatches with gold (empty = that method was right)")
    print(wrong.fillna("").to_string())


if __name__ == "__main__":
    main()
