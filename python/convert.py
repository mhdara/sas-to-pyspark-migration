"""convert.py - CODE migration: SAS program -> PySpark, then checks, sandboxed run, validation vs SAS
and at most ONE evidence-based repair.

Methods: llm_local (Ollama), llm_remote (Claude). The rule-based converter has its own script.
Run:  python python/convert.py --method llm_remote [--run 1] [program_id ...]   (no id = all programs)
Out:  converted/<method>/run<k>/p<program_id>.py             the converted code (one per attempt, last kept)
      data/parquet/py_out/<method>/run<k>/<table>/           the tables it wrote (git-ignored)
      outputs/conversion/attempts_<method>_run<k>.csv        one row per attempt (checks, time, tokens, cost)
      outputs/conversion/status_<method>_run<k>.csv          one row per program: VALIDATED / MANUAL_REVIEW
      outputs/conversion/validation_<method>_run<k>.csv      every validation check of the last attempts

Each program reads its inputs from the outputs of the SAME method's earlier programs (as in SAS), so an
error upstream shows downstream too.
"""

import argparse
import os
import py_compile
import re
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import yaml
from analyzer import strip_comments

ROOT = Path(__file__).resolve().parents[1]
C = yaml.safe_load((ROOT / "python" / "config.yaml").read_text())
MAP = yaml.safe_load((ROOT / "python" / "library_mappings.yaml").read_text())
PROVIDER_OF = {"llm_local": "ollama", "llm_remote": "anthropic"}
SKIP = {"00_load_raw": "REPLACED_BY_DATA_MIGRATION"}  # the loader is replaced by migrate_data.py


def prompt_template(kind: str) -> str:
    """The versioned conversion or repair prompt (prompts/<kind>_prompt_<version>.md)."""
    return (ROOT / "prompts" / f"{kind}_prompt_{C['versions'][kind + '_prompt']}.md").read_text(
        encoding="utf-8"
    )


def rule_blocks() -> list[tuple[list[str], str]]:
    """Each '## ' block of rules/conversion_rules.md with its trigger patterns."""
    text = (ROOT / "rules" / "conversion_rules.md").read_text(encoding="utf-8")
    blocks = []
    for b in re.split(r"\n(?=## )", text)[1:]:
        trig = re.search(r"triggers:\s*(.+)", b).group(1)
        blocks.append(([t.strip() for t in trig.split(", ")], b.strip()))
    return blocks


def targeted_rules(code: str) -> str:
    """Only the rule blocks whose trigger appears in this program (short, relevant prompts)."""
    return "\n\n".join(
        b for trig, b in rule_blocks() if any(re.search(t, code, re.IGNORECASE | re.MULTILINE) for t in trig)
    )


def clean_code(text: str) -> str:
    """The Python code from an LLM answer (removes a markdown fence if the model added one)."""
    m = re.search(r"```(?:python)?\n(.*?)```", text, re.DOTALL)
    return (m.group(1) if m else text).strip() + "\n"


def post_checks(path: Path, facts: dict, sas_code: str) -> list[str]:
    """Static checks before running: compiles, uses every table named in the SAS code, no RDDs, no
    absolute paths. Names built at run time (e.g. period_summary_202509 from &pid) are not required:
    correct code builds them the same way, so they never appear literally."""
    problems = []
    try:
        py_compile.compile(str(path), doraise=True)
    except py_compile.PyCompileError as e:
        problems.append(f"does not compile: {e.msg[:300]}")
    code = path.read_text()
    sas_logic = strip_comments(sas_code)  # names in comments (e.g. program headers) are not requirements
    for t in facts["inputs"] + facts["outputs"]:
        name = t.split(".")[1]
        if "&" not in t and re.search(rf"\b{name}\b", sas_logic, re.IGNORECASE) and name not in code:
            problems.append(f"table {t} not referenced")
    if ".rdd" in code:
        problems.append("uses the RDD API")
    if re.search(r"['\"](/home/|/Users/|/mnt/|[A-Za-z]:\\\\)", code):
        problems.append("absolute path in code")
    return problems


def run_sandboxed(path: Path, conv_dir: Path) -> tuple[bool, str]:
    """Run the converted script in its own process with a time limit; return (ok, end of stderr)."""
    env = dict(
        os.environ,
        PYTHONPATH=f"{conv_dir}{os.pathsep}{ROOT / 'python'}",
        PYSPARK_PYTHON=sys.executable,  # Spark workers must use the project's Python, not the system one
        PYSPARK_DRIVER_PYTHON=sys.executable,
    )
    try:
        p = subprocess.run(
            [sys.executable, str(path)],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=C["run"]["sandbox_timeout_s"],
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, "timeout"
    return p.returncode == 0, p.stderr[-2000:]


def facts_of(pid: str, deps: pd.DataFrame) -> dict:
    """The analyzer's facts the prompt needs: input/output tables, macros called, includes."""
    d = deps[deps.program_id == pid]
    tables = d[d.object_type == "TABLE"]
    return {
        "inputs": sorted(tables[tables.relationship == "READS"].object.unique()),
        "outputs": sorted(tables[tables.relationship == "WRITES"].object.unique()),
        "macros": sorted(d[d.relationship == "CALLS"].object.unique()),
        "includes": ["p" + x for x in d[d.relationship == "INCLUDES"].object.unique()],
    }


def library_status(status: list[dict], deps: pd.DataFrame) -> list[dict]:
    """A macro library writes no table, so it has no checks of its own: it counts as
    VALIDATED only when every program that %includes it is VALIDATED."""
    final = {s["program_id"]: s["final_status"] for s in status}
    libraries = set(deps[deps.relationship == "DEFINES"].program_id) - set(
        deps[(deps.relationship == "WRITES")].program_id
    )
    for s in status:
        if s["program_id"] in libraries and s["final_status"] == "VALIDATED":
            users = deps[(deps.relationship == "INCLUDES") & (deps.object == s["program_id"])].program_id
            ok = len(users) > 0 and all(final.get(u) == "VALIDATED" for u in users)
            s["final_status"] = "VALIDATED" if ok else "MANUAL_REVIEW"
            s["reason"] = (
                "validated through the programs that include it" if ok else "a program using it failed"
            )
    return status


def main():
    """Convert, run, validate and (at most once) repair each program with one method; save all results."""
    parser = argparse.ArgumentParser(description="convert SAS programs to PySpark and validate them")
    parser.add_argument("--method", required=True, choices=sorted(PROVIDER_OF))
    parser.add_argument("--run", type=int, default=1)
    parser.add_argument("programs", nargs="*")
    args = parser.parse_args()
    os.environ["LLM_PROVIDER"] = PROVIDER_OF[args.method]  # read by llm_client when it is imported
    from llm_client import LAST_USAGE, MODEL, chat
    from validate import py_out_dir, validate_program

    conv_dir = ROOT / "converted" / args.method / f"run{args.run}"
    out_dir = py_out_dir(args.method, args.run)
    results_dir = ROOT / "outputs" / "conversion"
    for folder in (conv_dir, out_dir, results_dir):
        folder.mkdir(parents=True, exist_ok=True)
    mappings = {"ctrl": MAP["ctrl"], "stg": MAP["stg"], "out": str(out_dir.relative_to(ROOT))}
    inv = pd.read_csv(ROOT / "outputs" / "migration_inventory.csv")
    deps = pd.read_csv(ROOT / "outputs" / "program_dependencies.csv")
    only = set(args.programs)
    attempts, status, checks = [], [], []

    for pid, rel_path in zip(inv.program_id, inv.path, strict=True):
        if only and pid not in only:
            continue
        if pid in SKIP:
            status.append({"program_id": pid, "final_status": SKIP[pid], "attempts": 0})
            continue
        code = (ROOT / rel_path).read_text(encoding="utf-8")
        facts = facts_of(pid, deps)
        rules = targeted_rules(code)
        prompt = prompt_template("conversion").format(
            program_id=pid, mappings=mappings, rules=rules, code=code, **facts
        )
        target = conv_dir / f"p{pid}.py"
        for attempt in range(1, 2 + C["run"]["max_repairs"]):
            text, latency = chat(prompt)
            target.write_text(clean_code(text), encoding="utf-8")
            for t in facts["outputs"]:  # remove this program's earlier outputs: validate fresh tables only
                shutil.rmtree(out_dir / t.split(".", 1)[1], ignore_errors=True)
            problems = post_checks(target, facts, code)
            ok_exec, err = (False, "; ".join(problems)) if problems else run_sandboxed(target, conv_dir)
            result = validate_program(pid, out_dir) if ok_exec else []
            failed = [c for c in result if not c["passed"]]
            ok_val = ok_exec and not failed
            attempts.append(
                {
                    "program_id": pid,
                    "method": args.method,
                    "run": args.run,
                    "attempt": attempt,
                    "post_checks_ok": int(not problems),
                    "execution_ok": int(ok_exec),
                    "validation_ok": int(ok_val),
                    "n_checks": len(result),
                    "n_failed_checks": len(failed),
                    "error": err[-500:].replace("\n", " ") if not ok_exec else "",
                    "model": MODEL,
                    "latency_s": latency,
                    **{k: LAST_USAGE.get(k) for k in ("input_tokens", "output_tokens", "cost_usd")},
                    "prompt_version": C["versions"]["conversion_prompt" if attempt == 1 else "repair_prompt"],
                    "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
                }
            )
            print(f"{pid} attempt {attempt}: exec={ok_exec} validation={ok_val} failed_checks={len(failed)}")
            if ok_val or attempt > C["run"]["max_repairs"]:
                break
            diffs = "; ".join(
                f"{c['table']}.{c.get('column', '')} {c['check']}: SAS={c['expected_value']} PY={c['actual_value']}"
                for c in failed[:15]
            )
            prompt = prompt_template("repair").format(
                program_id=pid,
                error=err if not ok_exec else "",
                diffs=diffs,
                inputs=facts["inputs"],
                outputs=facts["outputs"],
                mappings=mappings,
                rules=rules,
                code=code,
                previous=target.read_text(),
            )
        checks += [c | {"method": args.method, "run": args.run} for c in result]
        status.append(
            {
                "program_id": pid,
                "final_status": "VALIDATED" if ok_val else "MANUAL_REVIEW",
                "reason": "" if ok_val else ("execution_error" if not ok_exec else "validation_difference"),
                "attempts": attempt,
                "method": args.method,
                "run": args.run,
            }
        )
    status = library_status(status, deps)
    tag = f"{args.method}_run{args.run}"
    for name, rows in (("attempts", attempts), ("status", status), ("validation", checks)):
        path = results_dir / f"{name}_{tag}.csv"
        new = pd.DataFrame(rows)
        if only and path.exists():  # a partial run replaces only the programs it converted
            old = pd.read_csv(path)
            new = pd.concat([old[~old.program_id.isin(only)], new], ignore_index=True)
        new.to_csv(path, index=False)
    print(pd.DataFrame(status).to_string(index=False))


if __name__ == "__main__":
    main()
