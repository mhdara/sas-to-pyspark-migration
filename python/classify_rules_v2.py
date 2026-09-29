"""classify_rules_v2.py - deterministic classification with rules v2 (rules/classification_rules_v2.yaml).

v1.1 (classify_rules.py) stays the protocol result; v2 is broader and written after seeing v1.1.
Run:  python python/classify_rules_v2.py        (after python/analyzer.py)
Out:  outputs/rule_analysis_v2.csv  (labels, business confidence, score, reason)
Use:  classify(inventory, dependencies, codes) for any analyzed folder (tests).
"""

import re
from pathlib import Path

import pandas as pd
import yaml
from analyzer import strip_comments

ROOT = Path(__file__).resolve().parents[1]
RULES = yaml.safe_load((ROOT / "rules" / "classification_rules_v2.yaml").read_text(encoding="utf-8"))


def words(text: str) -> list[str]:
    """Split text into lower-case words at every non-letter/digit (loan_payments -> loan, payments)."""
    return re.findall(
        r"[a-z0-9]+", text.lower()
    )  # split at every non-letter: loan_payments -> loan, payments


def hits(keyword: str, tokens: list[str]) -> int:
    """Count how many words match a keyword: prefix match for 5+ letters, else the word or its plural."""
    if len(keyword) >= 5:  # stem: delinq -> delinquency
        return sum(t.startswith(keyword) for t in tokens)
    return sum(t in (keyword, keyword + "s") for t in tokens)  # short word: exact or plural


def facts_for(row: dict, code: str) -> dict:
    """Analyzer facts for one program plus the helpers the YAML rules can call (uses, name_matches)."""
    procs = set(filter(None, str(row.get("procs") or "").split("|")))
    f = dict(row)
    f["uses"] = lambda group: bool(procs & set(RULES["proc_groups"][group]))
    f["name_matches"] = lambda pattern: bool(re.search(pattern, row["program_id"], re.IGNORECASE))
    f["reads_raw_files"] = "IMPORT" in procs or bool(re.search(r"\binfile\b", code, re.IGNORECASE))
    return f


def technical(f: dict) -> tuple[str, str]:
    """Technical type: the first rule whose condition is true; returns (label, reason)."""
    for rule in RULES["technical"]:
        if eval(rule["if"], {}, f):  # rules come from our own versioned file, not user input
            note = f" ({rule['note']})" if "note" in rule else ""
            return rule["label"], f"technical: {rule['if']}{note}"
    raise ValueError("the last technical rule must be True")


def business(f: dict, code: str, outputs: list[str], inputs: list[str]) -> tuple[str, str, str]:
    """Label, confidence (HIGH / MEDIUM / LOW) and reason."""
    for rule in RULES["business"]["platform"]:
        if eval(rule["if"], {}, f):
            return "Platform", "HIGH", f"business: {rule['reason']}"
    w = RULES["business"]["weights"]
    sources = {  # table names without their library: out.loan_status -> loan, status
        "output_tables": words(" ".join(t.split(".", 1)[-1] for t in outputs)),
        "input_tables": words(" ".join(t.split(".", 1)[-1] for t in inputs)),
        "code": words(strip_comments(code)),
    }
    scores = {
        area: sum(w[src] * hits(k, tokens) for k in keywords for src, tokens in sources.items())
        for area, keywords in RULES["business"]["keywords"].items()
    }
    ranked = sorted(scores.items(), key=lambda kv: -kv[1])
    (best, top), (second, runner_up) = ranked[0], ranked[1]
    summary = ", ".join(f"{a} {s:g}" for a, s in ranked[:3])
    if top < RULES["business"]["min_score"]:
        return "Other", "LOW", f"business: weak evidence ({summary})"
    if top == runner_up:
        return "Other", "LOW", f"business: tie between {best} and {second} ({summary})"
    confidence = "HIGH" if top >= 2 * runner_up else "MEDIUM"
    return best, confidence, f"business: {summary}"


def complexity(f: dict) -> tuple[str, float, str]:
    """Complexity label, score and the three biggest contributors (line points are capped)."""
    c = RULES["complexity"]
    parts = {"lines": min(f["loc"] / 20 * c["loc_per_20"], c["loc_points_max"])}
    for fact, weight in c["weights"].items():
        if fact == "retain_or_first_last":
            value = bool(f["has_retain"] or f["has_first_last"])
        elif fact in RULES["proc_groups"]:
            value = f["uses"](fact)
        else:
            value = f[fact]
        parts[fact] = float(value) * weight
    score = round(sum(parts.values()), 2)
    t = c["thresholds"]
    label = "Complex" if score >= t["complex"] else "Moderate" if score >= t["moderate"] else "Simple"
    drivers = ", ".join(f"{k} {v:g}" for k, v in sorted(parts.items(), key=lambda kv: -kv[1])[:3] if v)
    return label, score, f"complexity drivers: {drivers or 'none'}"


def classify(inventory: pd.DataFrame, dependencies: pd.DataFrame, codes: dict[str, str]) -> pd.DataFrame:
    """Classify every program of an analyzed folder; returns one row per program with labels and reasons."""
    tables = dependencies[dependencies.object_type == "TABLE"]
    rows = []
    for row in inventory.to_dict("records"):
        pid, code = row["program_id"], codes[row["program_id"]]
        mine = tables[tables.program_id == pid]
        outputs = sorted(set(mine[mine.relationship == "WRITES"].object))
        inputs = sorted(set(mine[mine.relationship == "READS"].object) - set(outputs))
        f = facts_for(row, code)
        tech, tech_reason = technical(f)
        bus, bus_confidence, bus_reason = business(f, code, outputs, inputs)
        comp, score, comp_reason = complexity(f)
        rows.append(
            {
                "program_id": pid,
                "business_category": bus,
                "business_confidence": bus_confidence,
                "technical_category": tech,
                "complexity": comp,
                "complexity_score": score,
                "reason": f"{tech_reason}; {bus_reason}; {comp_reason}",
                "rules_version": RULES["version"],
            }
        )
    return pd.DataFrame(rows)


def main():
    """Classify the case programs with rules v2 and write outputs/rule_analysis_v2.csv."""
    inv = pd.read_csv(ROOT / "outputs" / "migration_inventory.csv").fillna({"procs": ""})
    deps = pd.read_csv(ROOT / "outputs" / "program_dependencies.csv")
    codes = {r.program_id: (ROOT / r.path).read_text(encoding="utf-8") for r in inv.itertuples()}
    out = classify(inv, deps, codes)
    out.to_csv(ROOT / "outputs" / "rule_analysis_v2.csv", index=False)
    cols = ["program_id", "business_category", "business_confidence", "technical_category", "complexity"]
    print(out[[*cols, "complexity_score"]].to_string(index=False))


if __name__ == "__main__":
    main()
