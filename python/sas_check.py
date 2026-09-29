"""sas_check.py - connect to SAS OnDemand and run sas/setup/check_environment.sas.

Run:  python python/sas_check.py
Out:  sas/logs/check_environment.log (+ .lst); the key lines are printed. Exit code 1 on any ERROR.
"""

import re
import sys

from sas_session import ROOT, connect, run_file

KEY_LINES = r"ROOT=|ENCODING=|CHECK OK|ETS_LICENSED=|ERROR|NOTE: SAS \(r\)|SAS \d"


def main() -> int:
    """Connect, run check_environment.sas, print the key log lines and the forecast test; exit code 1 on ERROR."""
    sas, root = connect()
    try:
        errors = run_file(sas, ROOT / "sas" / "setup" / "check_environment.sas")
        log = (ROOT / "sas" / "logs" / "check_environment.log").read_text(encoding="utf-8")
        print(f"connected, root = {root}\n")
        results = [ln for ln in log.splitlines() if not re.match(r"\d+ ", ln)]  # drop echoed source lines
        print("\n".join(ln for ln in results if re.search(KEY_LINES, ln)))
        lst = ROOT / "sas" / "logs" / "check_environment.lst"
        if lst.exists():
            print("\nforecast test (expect 150, 152, 154):\n" + lst.read_text(encoding="utf-8").strip())
    finally:
        sas.endsas()
    print(f"\n{len(errors)} ERROR line(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
