"""run_all.py - run the whole pipeline in order, stopping at the first failing step.

Run:  python python/run_all.py               deterministic steps only (default, a few minutes, free)
      python python/run_all.py --with-sas    also rerun SAS on SAS OnDemand (account + ~/.authinfo)
      python python/run_all.py --with-llm    also rerun the LLM steps (Ollama running + ANTHROPIC_API_KEY;
                                             costs about $1.20 and the local model takes hours)
Without the flags, the SAS results (sas/) and LLM results (outputs/, converted/) committed in the
repository are used as they are, so a reviewer can rerun everything else without SAS or an API key.
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / "python"

# (name, command, group): group None always runs; "sas" / "llm" only with the matching flag
STEPS = [
    ("1 data", ["generate_data.py"], None),
    ("2 SAS environment", ["sas_check.py"], "sas"),
    ("2 SAS load", ["sas_load.py"], "sas"),
    ("2 SAS programs", ["sas_run.py"], "sas"),
    ("3 analyzer", ["analyzer.py"], None),
    ("3 lineage back-trace", ["lineage_trace.py"], None),
    ("3 lineage diagram", ["lineage_diagram.py"], None),
    ("3 rules v1.1", ["classify_rules.py"], None),
    ("3 rules v2", ["classify_rules_v2.py"], None),
    ("3 LLM classification, local", ["analyze_llm.py"], "llm"),
    ("3 LLM classification, Claude", ["analyze_llm.py", "@anthropic"], "llm"),
    ("3 compare classifiers", ["compare.py"], None),
    ("4 data migration", ["migrate_data.py"], None),
    ("4 forecast parity", ["forecast.py", "--parity"], None),
    ("5 conversion, rules", ["convert_rules.py"], None),
    ("5 conversion, Claude", ["convert.py", "--method", "llm_remote"], "llm"),
    ("5 conversion, local", ["convert.py", "--method", "llm_local"], "llm"),
    ("5 conversion summary", ["conversion_summary.py"], None),
    ("6 Power BI tables", ["lineage_bi.py"], None),
]


def run(name: str, args: list[str]) -> float:
    """Run one step as its own process from the python/ folder; stop everything if it fails."""
    env = dict(os.environ)
    if "@anthropic" in args:  # the same script, with the remote provider
        args = [a for a in args if a != "@anthropic"]
        env["LLM_PROVIDER"] = "anthropic"
    print(f"\n=== {name}: {' '.join(args)}", flush=True)
    start = time.time()
    if subprocess.run(
        [sys.executable, str(PY / args[0]), *args[1:]], cwd=PY, env=env, check=False
    ).returncode:
        sys.exit(f"\nstopped: step '{name}' failed")
    return time.time() - start


def main():
    """Run the selected steps, then the test suite, and print the time each took."""
    parser = argparse.ArgumentParser(description="run the SAS to PySpark migration pipeline")
    parser.add_argument("--with-sas", action="store_true", help="rerun SAS on SAS OnDemand")
    parser.add_argument("--with-llm", action="store_true", help="rerun the LLM steps (cost, hours)")
    args = parser.parse_args()
    groups = {None} | ({"sas"} if args.with_sas else set()) | ({"llm"} if args.with_llm else set())
    timings = [(name, run(name, cmd)) for name, cmd, group in STEPS if group in groups]
    print("\n=== tests: pytest -q", flush=True)
    if subprocess.run([sys.executable, "-m", "pytest", "-q"], cwd=ROOT, check=False).returncode:
        sys.exit("\nstopped: tests failed")
    skipped = [name for name, _, group in STEPS if group not in groups]
    print("\nPipeline finished.")
    for name, seconds in timings:
        print(f"  {name:32s} {seconds:6.1f} s")
    if skipped:
        print(f"  skipped (committed results used): {', '.join(skipped)}")


if __name__ == "__main__":
    main()
