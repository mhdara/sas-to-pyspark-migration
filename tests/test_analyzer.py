"""Checks for the SAS static analyzer. Run from the project root: pytest -q"""

import itertools
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs"


def setup_module() -> None:
    """Run the analyzer once so every test reads fresh outputs."""
    subprocess.run([sys.executable, str(ROOT / "python" / "analyzer.py")], check=True, cwd=ROOT)


def deps(pid: str, relationship: str, object_type: str = "TABLE") -> set[str]:
    """Objects of one type that a program has a given relationship with (e.g. the tables it READS)."""
    d = pd.read_csv(OUT / "program_dependencies.csv")
    rows = d[(d.program_id == pid) & (d.relationship == relationship) & (d.object_type == object_type)]
    return set(rows.object)


def test_inventory_counts():
    """Key counts match what a person counts by reading the programs."""
    inv = pd.read_csv(OUT / "migration_inventory.csv").set_index("program_id")
    cols = ["n_inputs", "n_outputs", "n_macros_defined", "max_macro_nesting", "n_includes", "n_into"]
    expected = {  # hand-counted from the programs
        "00_load_raw": [1, 7, 0, 0, 0, 0],
        "04_payment_delinquency": [2, 2, 0, 0, 0, 1],
        "06_macro_library": [0, 0, 2, 0, 0, 0],
        "07_portfolio_report": [3, 2, 0, 1, 1, 1],
        "08_period_driver": [2, 5, 2, 3, 1, 1],
        "11_executive_report": [5, 2, 0, 0, 0, 1],
    }
    assert len(inv) == 12
    for pid, values in expected.items():
        assert inv.loc[pid, cols].tolist() == values, pid


def test_program_04_tables():
    """Program 04: exactly the two tables it reads and the two it writes."""
    assert deps("04_payment_delinquency", "READS") == {"ctrl.macro_parameters", "stg.loan_payments"}
    assert deps("04_payment_delinquency", "WRITES") == {"out.delinquency", "out.loan_status"}


def test_program_07_include_and_macro_calls():
    """Program 07: includes the macro library and calls both of its macros."""
    assert deps("07_portfolio_report", "INCLUDES", "PROGRAM") == {"06_macro_library"}
    assert deps("07_portfolio_report", "CALLS", "MACRO") == {"summarize", "flag_high"}


def test_program_08_dynamic_names_resolved_by_mprint():
    """Program 08: the &pid name is LOW from the code, the 4 real names MEDIUM from MPRINT."""
    d = pd.read_csv(OUT / "program_dependencies.csv")
    writes = d[(d.program_id == "08_period_driver") & (d.relationship == "WRITES")]
    confidence = dict(zip(writes.object, writes.confidence))
    assert confidence["out.period_summary_&pid"] == "LOW"
    for month in ("202509", "202510", "202511", "202512"):
        assert confidence[f"out.period_summary_{month}"] == "MEDIUM"


def test_written_tables_match_real_sas_outputs():
    """Static analysis vs ground truth: the out tables the code writes = the tables SAS produced."""
    d = pd.read_csv(OUT / "program_dependencies.csv")
    written = d[(d.relationship == "WRITES") & d.object.str.startswith("out.") & ~d.object.str.contains("&")]
    from_code = {o.removeprefix("out.") for o in written.object}
    from_sas = {f.stem for f in (ROOT / "sas" / "outputs").glob("*.sas7bdat")}
    assert from_code == from_sas


def test_lineage_traces_exec_report_back_to_loader():
    """The lineage graph links the executive report back to the raw loader, step by step."""
    e = pd.read_csv(OUT / "lineage_edges.csv")
    edges = set(zip(e.source_id, e.target_id))
    path = [
        "00_load_raw",
        "stg.loan_payments",
        "04_payment_delinquency",
        "out.loan_status",
        "11_executive_report",
        "out.exec_report",
    ]
    assert all((a, b) in edges for a, b in itertools.pairwise(path))


def test_source_files_are_linked_to_the_tables_they_load():
    """Data lineage starts at the raw files: each CSV loads exactly its own staging or settings table."""
    e = pd.read_csv(OUT / "lineage_edges.csv")
    loads = set(zip(e[e.relationship == "LOADS"].source_id, e[e.relationship == "LOADS"].target_id))
    assert ("loan_payments.csv", "stg.loan_payments") in loads
    assert ("ctrl_macro_parameters.csv", "ctrl.macro_parameters") in loads
    assert len(loads) == 7


def test_nested_macro_chain_crosses_files():
    """Code lineage: %period_driver -> %run_period -> %summarize (defined in 06, reached through %include)."""
    e = pd.read_csv(OUT / "lineage_edges.csv")
    calls = set(zip(e[e.relationship == "CALLS"].source_id, e[e.relationship == "CALLS"].target_id))
    assert {("run_period", "period_driver"), ("summarize", "run_period")} <= calls
    defines = set(zip(e[e.relationship == "DEFINES"].source_id, e[e.relationship == "DEFINES"].target_id))
    assert ("06_macro_library", "summarize") in defines


def test_back_trace_reaches_the_source_file_and_the_settings():
    """lineage_trace: the executive report traces back to the raw payments file and the threshold settings."""
    sys.path.insert(0, str(ROOT / "python"))
    from lineage_trace import load_edges, node_types, paths

    up = paths(load_edges(), node_types())
    behind = set(up[up.target_id == "out.exec_report"].upstream_id)
    assert {"loan_payments.csv", "ctrl.macro_parameters", "04_payment_delinquency", "00_load_raw"} <= behind
