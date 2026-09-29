"""lakebridge_run.py - run and validate the notebooks Databricks Lakebridge (Switch) produced.

Switch converts the SAS programs on Databricks (llm-transpile, see docs/TECHNICAL_REPORT.md). This script then treats
its output exactly like the other methods:
  1. export each notebook to converted/lakebridge_switch/run<k>/ (kept as produced)
  2. run it on Databricks serverless compute, in program order (inputs: workspace.stg / .ctrl Delta tables;
     outputs: workspace.out), after dropping the tables that program writes
  3. read its `out` tables back with their column types (SQL Statement Execution API)
  4. validate them with validate.py (same checks, same tolerances, same trap checks)
Run:  python python/lakebridge_run.py --ws-folder /Workspace/Users/<me>/sas2dbx/switch_sas \
        --method lakebridge_sas [--prompt "switch sas"] [--run 1]
Out:  outputs/conversion/{attempts,status,validation}_<method>_run<k>.csv
Needs the Databricks CLI with a profile (env DATABRICKS_PROFILE, default DEFAULT) and a SQL warehouse.
"""

import argparse
import base64
import json
import os
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from convert import library_status
from validate import outputs_of, py_out_dir, validate_program

ROOT = Path(__file__).resolve().parents[1]
METHOD = "lakebridge_switch"
PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
CATALOG = "workspace"


def cli(*args: str, payload: dict | None = None) -> dict:
    """Run a Databricks CLI command with JSON output (optionally a JSON request body)."""
    cmd = ["databricks", *args, "--profile", PROFILE, "-o", "json"]
    if payload is not None:
        cmd += ["--json", json.dumps(payload)]
    out = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if out.returncode:
        raise RuntimeError(f"{' '.join(args[:3])}: {out.stderr.strip()[-400:]}")
    return json.loads(out.stdout) if out.stdout.strip() else {}


def warehouse_id() -> str:
    """The first SQL warehouse of the workspace (used to run statements)."""
    return cli("warehouses", "list")[0]["id"]


def sql(statement: str, wid: str) -> pd.DataFrame:
    """Run SQL and return a DataFrame typed from the result schema (all result chunks)."""
    resp = cli("api", "post", "/api/2.0/sql/statements", payload={
        "statement": statement, "warehouse_id": wid, "wait_timeout": "50s",
        "disposition": "INLINE", "format": "JSON_ARRAY"})  # fmt: skip
    while resp["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        resp = cli("api", "get", f"/api/2.0/sql/statements/{resp['statement_id']}")
    if resp["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(resp["status"].get("error", {}).get("message", resp["status"]["state"]))
    columns = resp.get("manifest", {}).get("schema", {}).get("columns", [])
    rows, result = [], resp.get("result", {})
    while True:
        rows += result.get("data_array", []) or []
        link = result.get("next_chunk_internal_link")
        if not link:
            break
        result = cli("api", "get", link)
    df = pd.DataFrame(rows, columns=[c["name"] for c in columns])
    for c in columns:  # the API returns text: restore each column's type
        kind = c["type_name"]
        if kind in ("DOUBLE", "FLOAT", "DECIMAL", "INT", "LONG", "SHORT", "BYTE", "BIGINT", "INTEGER"):
            df[c["name"]] = pd.to_numeric(df[c["name"]])
        elif kind == "BOOLEAN":
            df[c["name"]] = df[c["name"]].map({"true": 1.0, "false": 0.0})
        elif kind == "DATE":
            df[c["name"]] = pd.to_datetime(df[c["name"]]).dt.date
    return df


def notebooks(ws_folder: str) -> dict[str, str]:
    """program_id -> workspace path of Switch's notebook for it (Switch names them after the input file)."""
    items = cli("workspace", "list", ws_folder)
    found = {}
    for item in items:
        name = Path(item["path"]).name.removesuffix(".py").removesuffix(".sas")
        pid = next((p for p in program_ids() if name.startswith(p)), None)
        if pid and item.get("object_type") == "NOTEBOOK":
            found[pid] = item["path"]
    return found


def program_ids() -> list[str]:
    """Programs 01-11 in run order (00 is replaced by the data migration)."""
    inv = pd.read_csv(ROOT / "outputs" / "migration_inventory.csv")
    return [p for p in inv.program_id if p != "00_load_raw"]


def export(path: str, target: Path) -> None:
    """Save a notebook's source locally, exactly as Switch produced it."""
    content = cli("workspace", "export", path, "--format", "SOURCE")["content"]
    target.write_bytes(base64.b64decode(content))


def run_notebook(path: str, pid: str, params: dict | None = None) -> tuple[bool, str, float]:
    """Run one notebook as a one-time serverless job run; return (ok, the notebook's real error, seconds)."""
    start = time.time()
    run_id = cli("jobs", "submit", "--no-wait", payload={
        "run_name": f"sas2dbx {METHOD} {pid}",
        "tasks": [{"task_key": "run", "notebook_task": {"notebook_path": path,
                                                         "base_parameters": params or {}}}]})["run_id"]  # fmt: skip
    while True:
        run = cli("jobs", "get-run", str(run_id))
        if run["state"]["life_cycle_state"] in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        time.sleep(10)
    seconds = round(time.time() - start, 1)
    if run["state"].get("result_state") == "SUCCESS":
        return True, "", seconds
    task_run = run["tasks"][-1]["run_id"]  # the last attempt of the task holds the notebook's error
    output = cli("jobs", "get-run-output", str(task_run))
    error = output.get("error") or run["state"].get("state_message", "")
    return False, str(error).split("\n")[0][-500:], seconds


def main():
    """Export, run, read back and validate every Switch notebook; write the method's result files."""
    parser = argparse.ArgumentParser(description="run and validate Lakebridge Switch notebooks")
    parser.add_argument("--ws-folder", required=True)
    parser.add_argument("--run", type=int, default=1)
    parser.add_argument(
        "--method", default="lakebridge_switch", help="method name in results, e.g. lakebridge_sas"
    )
    parser.add_argument("--prompt", default="switch unknown_etl", help="the Switch source dialect used")
    parser.add_argument(
        "--params",
        default="{}",
        help='notebook parameters, path-like only (sensitivity run), e.g. {"root": "..."}',
    )
    args = parser.parse_args()
    global METHOD
    METHOD = args.method
    conv_dir, out_dir = ROOT / "converted" / METHOD / f"run{args.run}", py_out_dir(METHOD, args.run)
    for folder in (conv_dir, out_dir):
        folder.mkdir(parents=True, exist_ok=True)
    wid, found = warehouse_id(), notebooks(args.ws_folder)
    attempts, status, checks = [], [], []
    for pid in program_ids():
        if pid not in found:
            status.append({"program_id": pid, "final_status": "MANUAL_REVIEW", "reason": "no notebook produced",
                           "attempts": 0, "method": METHOD, "run": args.run})  # fmt: skip
            print(f"{pid}: no notebook")
            continue
        export(found[pid], conv_dir / f"p{pid}.py")
        tables = outputs_of(pid)
        for t in tables:  # validate fresh outputs only
            sql(f"DROP TABLE IF EXISTS {CATALOG}.out.{t}", wid)
        ok, err, seconds = run_notebook(found[pid], pid, json.loads(args.params))
        result = []
        if ok:
            for t in tables:
                exists = sql(f"SHOW TABLES IN {CATALOG}.out LIKE '{t}'", wid)
                if len(exists):
                    (out_dir / t).mkdir(parents=True, exist_ok=True)
                    sql(f"SELECT * FROM {CATALOG}.out.{t}", wid).to_parquet(out_dir / t / "part-0.parquet")
            result = validate_program(pid, out_dir)
        failed = [c for c in result if not c["passed"]]
        ok_val = ok and not failed
        attempts.append({"program_id": pid, "method": METHOD, "run": args.run, "attempt": 1,
                         "post_checks_ok": 1, "execution_ok": int(ok), "validation_ok": int(ok_val),
                         "n_checks": len(result), "n_failed_checks": len(failed),
                         "error": err.replace("\n", " "), "model": "databricks-gpt-oss-120b (Switch)",
                         "latency_s": seconds, "input_tokens": None, "output_tokens": None, "cost_usd": None,
                         "prompt_version": args.prompt,
                         "timestamp": datetime.now(UTC).isoformat(timespec="seconds")})  # fmt: skip
        checks += [c | {"method": METHOD, "run": args.run} for c in result]
        status.append({"program_id": pid, "final_status": "VALIDATED" if ok_val else "MANUAL_REVIEW",
                       "reason": "" if ok_val else ("execution_error" if not ok else "validation_difference"),
                       "attempts": 1, "method": METHOD, "run": args.run})  # fmt: skip
        print(f"{pid}: exec={ok} validation={ok_val} failed_checks={len(failed)} ({seconds} s)")
    deps = pd.read_csv(ROOT / "outputs" / "program_dependencies.csv")
    status = library_status(status, deps)
    tag = f"{METHOD}_run{args.run}"
    for name, rows in (("attempts", attempts), ("status", status), ("validation", checks)):
        pd.DataFrame(rows).to_csv(ROOT / "outputs" / "conversion" / f"{name}_{tag}.csv", index=False)
    print(pd.DataFrame(status).to_string(index=False))


if __name__ == "__main__":
    main()
