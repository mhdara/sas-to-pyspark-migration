"""sas_session.py - the ONLY place that talks to SAS OnDemand (through SASPy).

connect()   opens a session, defines &home and &root (= <ODA home>/case) and creates the project folders
run_file()  submits one .sas file, saves its log and listing in sas/logs/, returns the ERROR lines
"""

from __future__ import annotations

import re
from pathlib import Path

import saspy

ROOT = Path(__file__).resolve().parents[1]
CFG_FILE = ROOT / "sas" / "sascfg_personal.py"
LOG_DIR = ROOT / "sas" / "logs"
FOLDERS = ["raw", "stg", "ctrl", "out", "logs", "programs", "v1", "v2"]


def connect() -> tuple[saspy.SASsession, str]:
    """Open an ODA session. Replaces the SAS Studio autoexec, which SASPy sessions do not run."""
    sas = saspy.SASsession(cfgname="oda", cfgfile=str(CFG_FILE), results="TEXT")
    sas.submit("%let home = %sysget(HOME);")
    root = f"{sas.symget('home')}/case"
    sas.symput("root", root)
    folders = ", ".join(f"'{f}'" for f in FOLDERS)
    sas.submit(f"""
        data _null_;  /* DCREATE returns blank when the folder already exists: safe to rerun */
          length name $32 path $256;
          path = dcreate('case', "&home");
          do name = {folders};
            path = dcreate(strip(name), "&root");
          end;
        run;
    """)
    return sas, root


def run_file(sas: saspy.SASsession, sas_file: Path, log_name: str | None = None) -> list[str]:
    """Submit a SAS program; write <log_name>.log (+ .lst when there is printed output)."""
    log_name = log_name or sas_file.stem
    result = sas.submit(sas_file.read_text(encoding="utf-8"))
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    (LOG_DIR / f"{log_name}.log").write_text(result["LOG"], encoding="utf-8")
    if result["LST"].strip():
        (LOG_DIR / f"{log_name}.lst").write_text(result["LST"], encoding="utf-8")
    return [line for line in result["LOG"].splitlines() if re.match(r"\s*ERROR", line)]
