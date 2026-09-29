"""sas_run.py - plan Phases 4-5: run SAS programs 01-11 on SAS OnDemand and download the results.

Run:  python python/sas_run.py            (after python/sas_load.py has loaded stg and ctrl)
Does: 1. upload sas/programs/01-11 to &root/programs (07 and 08 %include 06 from there)
      2. empty the out library, so only tables made by this run are downloaded
      3. run the programs in order; stop at the first one whose log has an ERROR
      4. download every out table to sas/outputs/ and the 08 MPRINT file to sas/logs/
Out:  sas/logs/<program>.log (+ .lst), sas/logs/08_period_driver_mprint.sas, sas/outputs/*.sas7bdat
"""

import re
import sys

from sas_session import LOG_DIR, ROOT, connect, run_file

PROGRAMS = sorted(p for p in (ROOT / "sas" / "programs").glob("*.sas") if not p.name.startswith("00_"))
OUTPUTS = ROOT / "sas" / "outputs"
MPRINT = "08_period_driver_mprint.sas"


def main():
    """Upload and run programs 01-11 in order (stop at the first ERROR), then download all results."""
    sas, root = connect()
    try:
        for p in PROGRAMS:
            sas.upload(str(p), f"{root}/programs/{p.name}")
        sas.submit('libname out "&root/out"; proc datasets library=out kill nolist; quit;')

        for p in PROGRAMS:
            errors = run_file(sas, p)
            log = (LOG_DIR / f"{p.stem}.log").read_text(encoding="utf-8")
            warnings = [ln for ln in log.splitlines() if re.match(r"WARNING", ln)]
            print(f"{p.stem:24s} {len(errors)} ERROR, {len(warnings)} WARNING")
            for line in warnings:
                print(f"    {line}")
            if errors:
                print("\n".join(f"    {e}" for e in errors))
                sys.exit(f"stopped at {p.stem}: see sas/logs/{p.stem}.log")

        OUTPUTS.mkdir(exist_ok=True)
        for old in OUTPUTS.glob("*.sas7bdat"):
            old.unlink()
        tables = sorted(f for f in sas.dirlist(f"{root}/out") if f.endswith(".sas7bdat"))
        for name in tables:
            sas.download(str(OUTPUTS / name), f"{root}/out/{name}")
        sas.download(str(LOG_DIR / MPRINT), f"{root}/logs/{MPRINT}")
        print(f"\n{len(tables)} out tables downloaded to sas/outputs/, MPRINT file to sas/logs/")
        print("  " + ", ".join(t.removesuffix(".sas7bdat") for t in tables))
    finally:
        sas.endsas()


if __name__ == "__main__":
    main()
