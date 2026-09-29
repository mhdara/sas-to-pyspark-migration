"""classify_rules.py - deterministic classification from analyzer facts + versioned YAML rules.
Run: python python/classify_rules.py     Out: outputs/rule_analysis.csv (labels + score + reason)"""

import re
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
RULES = yaml.safe_load((ROOT / "rules" / "classification_rules_v1.yaml").read_text(encoding="utf-8"))


def technical(f):
    """Technical type: the first rule in the YAML whose condition is true for these facts."""
    for r in RULES["technical"]:
        if eval(r["if"], {}, f):  # rules are our own file, not user input
            return r["label"], f"technical rule: {r['if']}"


def business(code, f):
    """Business area: macro-only and INFILE programs are Platform; otherwise the area with most keyword hits."""
    text = re.sub(r"/\*.*?\*/", " ", code, flags=re.DOTALL).lower()
    scores = {
        k: sum(len(re.findall(re.escape(w), text)) for w in words)
        for k, words in RULES["business"].items()
        if k != "Platform"
    }
    if f["n_macros_defined"] >= 1 and f["n_outputs"] == 0:
        return "Platform", "business rule: macro-only program"
    if re.search(r"\binfile\b", text):
        return "Platform", "business rule: raw-file ingestion (INFILE)"
    best = max(scores, key=scores.get)
    return best, f"business keyword score {scores[best]} ({best})"


def complexity(f):
    """Complexity label and score: weighted sum of analyzer facts, compared with the YAML thresholds."""
    w = RULES["complexity"]["weights"]
    score = (
        f["loc"] / 20 * w["loc_per_20"]
        + f["n_macros_defined"] * w["n_macros_defined"]
        + f["max_macro_nesting"] * w["max_macro_nesting"]
        + f["n_into"] * w["n_into"]
        + f["n_includes"] * w["n_includes"]
        + f["n_joins"] * w["n_joins"]
        + (f["has_retain"] or f["has_first_last"]) * w["retain_or_first_last"]
        + f["has_format"] * w["has_format"]
        + f["has_reg"] * w["has_reg"]
        + f["has_forecast"] * w["has_forecast"]
        + f["n_dynamic_names"] * w["n_dynamic_names"]
    )
    t = RULES["complexity"]["thresholds"]
    label = "Complex" if score >= t["complex"] else "Moderate" if score >= t["moderate"] else "Simple"
    return label, round(score, 2)


def main():
    """Classify every program in the inventory with rules v1.1 and write outputs/rule_analysis.csv."""
    inv = pd.read_csv(ROOT / "outputs" / "migration_inventory.csv").fillna({"procs": ""})
    rows = []
    for f in inv.to_dict("records"):
        code = (ROOT / f["path"]).read_text(encoding="utf-8")
        tech, tech_reason = technical(f)
        bus, bus_reason = business(code, f)
        comp, score = complexity(f)
        rows.append(
            {
                "program_id": f["program_id"],
                "business_category": bus,
                "technical_category": tech,
                "complexity": comp,
                "complexity_score": score,
                "reason": f"{tech_reason}; {bus_reason}",
                "rules_version": RULES["version"],
            }
        )
    out = pd.DataFrame(rows)
    out.to_csv(ROOT / "outputs" / "rule_analysis.csv", index=False)
    print(
        out[
            ["program_id", "business_category", "technical_category", "complexity", "complexity_score"]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()
