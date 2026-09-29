"""sas_load.py - load the CSV files into SAS OnDemand, with the truncation demo.

Run:  python python/sas_load.py
Does: 1. upload data/raw/*.csv and 00_load_raw.sas
      2. run 00_load_raw.sas once per version: v1 = customer_name $12 (too short), v2 = $60 (correct)
         and keep each version's customers table in case/v<i> (server) and sas/data/v<i> (Mac)
      3. download stg and ctrl (from the last run, v2) and check them against the CSV files
Out:  sas/data/{stg,ctrl,v1,v2}/*.sas7bdat, sas/logs/00_load_raw_v<i>.{log,lst}, sas/logs/truncation_demo.csv
Exit code 1 if a SAS log has an ERROR or a check fails.
"""

import sys

import pandas as pd
import pyreadstat
from sas_session import ROOT, connect, run_file

RAW = ROOT / "data" / "raw"
PROGRAM = ROOT / "sas" / "programs" / "00_load_raw.sas"
SAS_DATA = ROOT / "sas" / "data"
LOG_DIR = ROOT / "sas" / "logs"
TABLES = {  # SAS library -> {table: CSV it is loaded from}
    "ctrl": {t: f"ctrl_{t}.csv" for t in ("client_segments", "reporting_periods", "macro_parameters")},
    "stg": {t: f"{t}.csv" for t in ("customers", "loans", "loan_payments", "card_transactions")},
}
VERSIONS = {"v1": "$12", "v2": "$60"}  # customer_name length per run; the last one stays in stg
LENGTH_LINE = "%let name_len = $60;"  # the line in 00_load_raw.sas that each run replaces
SHOWCASE_IDS = [8, 43, 44, 45]  # planted names (data_specs.md, cases 6 and 7)


def download(sas, root, folder, table):
    """Download one table from the server folder to sas/data/<folder>/; stop the run if it fails."""
    target = SAS_DATA / folder / f"{table}.sas7bdat"
    target.parent.mkdir(parents=True, exist_ok=True)
    if not sas.download(str(target), f"{root}/{folder}/{table}.sas7bdat")["Success"]:
        sys.exit(f"download of {folder}/{table} failed")


def read_sas(path):
    """Read a SAS table byte-safe: a name cut inside a 2-byte character is invalid UTF-8, which a
    plain UTF-8 read refuses. latin1 reads every byte; the text is then decoded as UTF-8 with any
    broken character shown as U+FFFD."""
    df, meta = pyreadstat.read_sas7bdat(str(path), encoding="latin1")
    for c in df.columns:
        if df[c].map(lambda v: isinstance(v, str)).all():
            df[c] = df[c].map(lambda s: s.encode("latin1").decode("utf-8", errors="replace").rstrip())
    return df, meta


def run_version(sas, root, version, length, code):
    """One run of the loader; its customers table is copied to case/<version> and downloaded."""
    script = LOG_DIR / f".00_load_raw_{version}.sas"  # temporary copy with this version's length
    script.write_text(code.replace(LENGTH_LINE, f"%let name_len = {length};"), encoding="utf-8")
    try:
        errors = run_file(sas, script, f"00_load_raw_{version}")
    finally:
        script.unlink()
    ids = " ".join(map(str, SHOWCASE_IDS))
    listing = sas.submit(f"""
        libname {version} "&root/{version}";
        proc copy in=stg out={version} memtype=data; select customers; run;
        title "00_load_raw {version}: planted names as stored";
        proc print data={version}.customers noobs; where customer_id in ({ids});
          var customer_id customer_name city; run;
        title; libname {version} clear;
    """)["LST"]
    with (LOG_DIR / f"00_load_raw_{version}.lst").open("a", encoding="utf-8") as f:
        f.write("\n" + listing)
    download(sas, root, version, "customers")
    return errors


def check_against_csv():
    """stg and ctrl tables (final load) vs their CSV: row counts and every text value."""
    failures = []
    for lib, tables in TABLES.items():
        for table, csv_name in tables.items():
            sas_df, _ = read_sas(SAS_DATA / lib / f"{table}.sas7bdat")
            csv_df = pd.read_csv(RAW / csv_name, dtype=str, keep_default_na=False)
            text = [c for c in sas_df.columns if c in csv_df and sas_df[c].map(type).eq(str).all()]
            ok = len(sas_df) == len(csv_df) and all((sas_df[c] == csv_df[c]).all() for c in text)
            print(f"  {'OK' if ok else 'FAIL':4s} {lib}.{table:18s} {len(sas_df):>6} rows, text identical")
            failures += [] if ok else [f"{lib}.{table}"]
    return failures


def compare_versions():
    """One line per version (width, label, names cut) + truncation_demo.csv with the planted names."""
    csv = pd.read_csv(RAW / "customers.csv").set_index("customer_id").customer_name
    demo = pd.DataFrame({"customer_id": SHOWCASE_IDS, "csv": csv[SHOWCASE_IDS].values})
    failures = []
    for version, length in VERSIONS.items():
        df, meta = read_sas(SAS_DATA / version / "customers.sas7bdat")
        names = df.set_index("customer_id").customer_name
        cut, broken = int((names != csv[names.index]).sum()), int(names.str.contains("�").sum())
        width = meta.variable_storage_width["customer_name"]
        print(
            f"  {version}: width {width}, label {meta.file_label!r}, "
            f"{cut} of {len(names)} names cut, {broken} cut inside a character"
        )
        demo[version] = names[SHOWCASE_IDS].values
        if width != int(length[1:]) or length not in (meta.file_label or ""):
            failures.append(f"{version} table")
    demo.to_csv(LOG_DIR / "truncation_demo.csv", index=False)
    print(demo.to_string(index=False))
    if (demo.v1 == demo.csv).all() or (demo.v2 != demo.csv).any():
        failures.append("truncation demo: v1 must cut names, v2 must keep all")
    return failures


def main():
    """Upload the CSVs and the loader, run v1 and v2, download stg and ctrl, then run every check."""
    code = PROGRAM.read_text(encoding="utf-8")
    if LENGTH_LINE not in code:
        sys.exit(f"{PROGRAM.name} must contain the line: {LENGTH_LINE}")
    sas, root = connect()
    errors = []
    try:
        for csv_name in (n for tables in TABLES.values() for n in tables.values()):
            sas.upload(str(RAW / csv_name), f"{root}/raw/{csv_name}")
        sas.upload(str(PROGRAM), f"{root}/programs/{PROGRAM.name}")
        for version, length in VERSIONS.items():
            errors += run_version(sas, root, version, length, code)
            print(f"{version} (customer_name {length}): loaded, copied to case/{version}")
        for lib, tables in TABLES.items():
            for table in tables:
                download(sas, root, lib, table)
    finally:
        sas.endsas()

    print("\nSAS tables vs CSV:")
    failures = check_against_csv()
    print("\nversions:")
    failures += compare_versions()
    print(f"\n{len(errors)} ERROR line(s) in SAS logs, {len(failures)} failed check(s) {failures or ''}")
    return 1 if errors or failures else 0


if __name__ == "__main__":
    sys.exit(main())
