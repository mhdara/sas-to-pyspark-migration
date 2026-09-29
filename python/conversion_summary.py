"""conversion_summary.py - compare the code-conversion methods.

Reads every outputs/conversion/{status,attempts,validation}_<method>_run<k>.csv and writes:
  outputs/conversion/summary_by_method.csv    one row per method and run: validated, first-attempt passes,
                                              repairs, not supported, planted traps passed, time, cost
  outputs/conversion/status_grid.csv          one row per program, one column per method and run
Run:  python python/conversion_summary.py
"""

import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CONV = ROOT / "outputs" / "conversion"


def load(kind: str) -> pd.DataFrame:
    """All result files of one kind, with method and run taken from the file name."""
    frames = []
    for path in sorted(CONV.glob(f"{kind}_*_run*.csv")):
        method, run = re.fullmatch(rf"{kind}_(.+)_run(\d+)", path.stem).groups()
        frames.append(pd.read_csv(path).assign(method=method, run=int(run)))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main():
    """Summarise each method and run, and print the program x method grid."""
    status = load("status")
    status = status[status.program_id != "00_load_raw"]
    attempts, checks = load("attempts"), load("validation")
    rows = []
    for (method, run), s in status.groupby(["method", "run"]):
        a = attempts[(attempts.method == method) & (attempts.run == run)]
        c = checks[(checks.method == method) & (checks.run == run)]
        validated = s.final_status == "VALIDATED"
        first = int(
            (validated & (s.attempts <= 1)).sum()
        )  # final status counts (a library can be downgraded)
        planted = c[c.check.str.startswith("planted_")] if len(c) else c
        rows.append(
            {
                "method": method,
                "run": run,
                "programs": len(s),
                "validated": int((s.final_status == "VALIDATED").sum()),
                "validated_first_attempt": int(first),
                "validated_after_repair": int(((s.final_status == "VALIDATED") & (s.attempts > 1)).sum()),
                "manual_review": int((s.final_status == "MANUAL_REVIEW").sum()),
                "not_supported": int((s.final_status == "NOT_SUPPORTED").sum()),
                "checks_passed": f"{int(c.passed.sum()) if len(c) else 0}/{len(c)}",
                "planted_traps_passed": f"{int(planted.passed.sum()) if len(planted) else 0}/{len(planted)}",
                "llm_calls": len(a[a.model != "rules"]) if len(a) else 0,
                "minutes": round(a.latency_s.sum() / 60, 1) if len(a) else 0.0,
                "cost_usd": round(a.cost_usd.fillna(0).sum(), 3) if len(a) else 0.0,
            }
        )
    summary = pd.DataFrame(rows)
    grid = status.assign(col=status.method + " run" + status.run.astype(str)).pivot(
        index="program_id", columns="col", values="final_status"
    )
    summary.to_csv(CONV / "summary_by_method.csv", index=False)
    grid.to_csv(CONV / "status_grid.csv")
    print(summary.to_string(index=False))
    print()
    print(grid.fillna("-").to_string())


if __name__ == "__main__":
    main()
