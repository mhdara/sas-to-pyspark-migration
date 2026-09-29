"""analyze_llm.py - LLM analysis of every SAS program (interprets the analyzer facts).
Run: python python/analyze_llm.py [program_id ...]     (no argument = all programs)
Out: outputs/llm_analysis_<model>.csv  (one file per model, e.g. llm_analysis_qwen2.5-coder_14b.csv)
The provider and model come from config.yaml (llm.provider)."""

import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
from jsonschema import ValidationError, validate
from llm_client import LAST_USAGE, MODEL, PROVIDER, chat

ROOT = Path(__file__).resolve().parents[1]
VER = yaml.safe_load((ROOT / "python" / "config.yaml").read_text())["versions"]
PROMPT = (ROOT / "prompts" / f"analysis_prompt_{VER['analysis_prompt']}.md").read_text(encoding="utf-8")
SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "business_category",
        "technical_category",
        "complexity",
        "complexity_reason",
        "business_description",
        "technical_description",
    ],
    "properties": {
        "business_category": {
            "enum": ["Lending", "Payments", "Customer Management", "Risk", "Reporting", "Platform", "Other"]
        },
        "technical_category": {
            "enum": [
                "Data Preparation",
                "Transformation",
                "Aggregation",
                "Statistical Modeling",
                "Forecasting",
                "Reporting",
                "Orchestration",
                "Utility",
            ]
        },
        "complexity": {"enum": ["Simple", "Moderate", "Complex"]},
        "complexity_reason": {"type": "string"},
        "business_description": {"type": "string"},
        "technical_description": {"type": "string"},
    },
}


def facts_for(pid, inv, deps):
    """The analyzer's facts about one program as JSON for the prompt: inventory row + input/output tables + macros called."""
    f = inv[inv.program_id == pid].iloc[0].to_dict()
    d = deps[deps.program_id == pid]
    f["inputs"] = sorted(d[(d.object_type == "TABLE") & (d.relationship == "READS")].object.unique())
    f["outputs"] = sorted(d[(d.object_type == "TABLE") & (d.relationship == "WRITES")].object.unique())
    f["macros_called"] = sorted(d[d.relationship == "CALLS"].object.unique())
    return json.dumps(
        {k: (v if not hasattr(v, "item") else v.item()) for k, v in f.items()}, indent=1, default=str
    )


def main(only=None):
    """Classify each program (or only the given ones) with the configured LLM and save one CSV per model.

    Each answer is checked against SCHEMA; invalid JSON gets one retry, then is recorded as INVALID OUTPUT.
    """
    inv = pd.read_csv(ROOT / "outputs" / "migration_inventory.csv").fillna("")
    if only:
        inv = inv[inv.program_id.isin(only)]
    deps = pd.read_csv(ROOT / "outputs" / "program_dependencies.csv")
    rows = []
    for pid, path in zip(inv.program_id, inv.path):
        code = (ROOT / path).read_text(encoding="utf-8")
        prompt = PROMPT.format(facts=facts_for(pid, inv, deps), program_id=pid, code=code)
        for attempt in (1, 2):  # one retry only if the JSON is invalid
            try:
                out, latency = chat(prompt, SCHEMA)
                validate(out, SCHEMA)
                break
            except (json.JSONDecodeError, ValidationError) as e:  # connection errors stop the run
                out, latency = {"complexity_reason": f"INVALID OUTPUT: {e}"}, None
        rows.append(
            dict(
                program_id=pid,
                **out,
                provider=PROVIDER,
                model=MODEL,
                prompt_version=VER["analysis_prompt"],
                analyzer_version=VER["analyzer"],
                script_sha256=hashlib.sha256(code.encode()).hexdigest(),
                latency_s=latency,
                **{
                    k: LAST_USAGE.get(k) for k in ("input_tokens", "output_tokens", "cost_usd", "answered_by")
                },
                timestamp=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            )
        )
        print(pid, out.get("business_category"), out.get("technical_category"), out.get("complexity"))
    target = ROOT / "outputs" / f"llm_analysis_{re.sub(r'[^A-Za-z0-9.]+', '_', MODEL)}.csv"
    pd.DataFrame(rows).to_csv(target, index=False)
    print(f"-> {target.relative_to(ROOT)}")


if __name__ == "__main__":
    main(sys.argv[1:] or None)
