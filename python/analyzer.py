"""analyzer.py - Migration Inventory + SAS static dependency analyzer.

Pattern-based (regex on SAS statements), NOT a full SAS grammar - say so in the report.
Every edge points in the direction things FLOW (data or code):
  TABLE -READS-> PROGRAM, PROGRAM -WRITES-> TABLE, LIBRARY -USES_LIBRARY-> PROGRAM,
  PROGRAM -DEFINES-> MACRO, MACRO -CALLS-> PROGRAM, PROGRAM -INCLUDES-> PROGRAM (included -> includer)
Confidence: HIGH = literal name in code, MEDIUM = from macro-call arguments or MPRINT, LOW = unresolved (&var).

Run:  python python/analyzer.py
Out:  outputs/migration_inventory.csv, program_dependencies.csv, lineage_nodes.csv, lineage_edges.csv
Use:  analyze(prog_dir, log_dir) returns the same four tables for any folder of .sas files (tests).
"""

import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROG_DIR, LOG_DIR, OUT_DIR = ROOT / "sas" / "programs", ROOT / "sas" / "logs", ROOT / "outputs"
ANALYZER_VERSION = (
    "1.2"  # 1.1: extra pattern facts + n_upstream_programs; 1.2: source files, macro-to-macro calls
)

TABLE = r"([A-Za-z_&][\w&.]*\.[\w&.]+)"  # two-level name lib.table (may contain &var)


PATTERN_FACTS = {  # yes/no facts found anywhere in the code (comments removed), used by rules v2
    "has_call_execute": r"\bcall\s+execute\b",  # generates and runs SAS code from data
    "has_symput": r"\bcall\s+symputx?\b",  # data values -> macro variables
    "has_hash": r"\b(declare|dcl)\s+hash\b",  # in-memory lookup tables in a DATA step
    "has_array": r"\barray\s+\w+",  # DATA step arrays
    "has_passthrough": r"\bconnect\s+to\b",  # SQL sent to an external database
    "has_file_output": r"\bfile\s+[\w\"']|\bods\s+(pdf|excel|html|rtf|csv|tagsets)",  # files/reports
    "has_group_by": r"\bgroup\s+by\b",  # SQL aggregation
}


def strip_comments(code):
    """Remove /* ... */ and '* ...;' comments, keeping line breaks so line counts stay meaningful."""
    code = re.sub(r"/\*.*?\*/", lambda m: "\n" * m.group(0).count("\n"), code, flags=re.DOTALL)
    return re.sub(r"(^|;)(\s*)\*[^;]*;", r"\1\2", code)  # "* comment;" statements


def statements(code):
    """Split code into SAS statements (each ends with ';'), with whitespace collapsed."""
    return [re.sub(r"\s+", " ", s).strip() for s in code.split(";") if s.strip()]


def is_table(name):
    """True for a permanent two-level name (lib.table); WORK tables are temporary and not part of lineage."""
    lib = name.split(".")[0].lower()
    return "." in name and lib not in ("work",) and not name[0].isdigit()


def parse_program(path):
    """Read one .sas file and extract its facts: tables read/written, libraries, procedures, macros, includes, counts.

    Works statement by statement with regular expressions. Macro calls are only collected here; they are
    matched against the macros defined in ALL programs later, in analyze().
    """
    raw = path.read_text(encoding="utf-8")
    code = strip_comments(raw)
    st = statements(code)
    f = {
        "program_id": path.stem,
        "program_name": path.name,
        "path": str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path),
        "size_bytes": path.stat().st_size,
        "loc": sum(1 for line in code.splitlines() if line.strip()),
        "reads": set(),
        "writes": set(),
        "libraries": {},
        "procs": [],
        "macros_defined": {},
        "open_calls": [],
        "includes": [],
        "into_vars": [],
        "n_data_steps": 0,
        "n_joins": 0,
        "dynamic": set(),
        "call_args": [],
        "filerefs": {},  # FILENAME fileref -> file name
        "files_read": set(),  # external files read with INFILE (e.g. the source CSVs)
        "file_loads": set(),  # (file, table): the DATA step that reads the file writes the table
    }
    step_writes = []  # tables written by the current DATA step
    in_sql, cur_macro = False, None
    for s in st:
        low = s.lower()
        if m := re.match(r"%macro\s+(\w+)", low):
            cur_macro = m.group(1)
            f["macros_defined"][cur_macro] = set()
            continue
        if low.startswith("%mend"):
            cur_macro = None
            continue
        if m := re.match(r"libname\s+(\w+)\s+[\"']([^\"']+)", s, re.IGNORECASE):
            f["libraries"][m.group(1).lower()] = m.group(2)
        if m := re.match(r"filename\s+(\w+)\s+[\"']([^\"']+)[\"']", s, re.IGNORECASE):
            f["filerefs"][m.group(1).lower()] = Path(m.group(2)).name
        if m := re.match(r"infile\s+(?:[\"']([^\"']+)[\"']|(\w+))", s, re.IGNORECASE):
            file = Path(m.group(1)).name if m.group(1) else f["filerefs"].get(m.group(2).lower(), m.group(2))
            f["files_read"].add(file)
            f["file_loads"].update((file, t) for t in step_writes)
        if m := re.match(r"%include\s+[\"']([^\"']+)[\"']", s, re.IGNORECASE):
            f["includes"].append(Path(m.group(1)).stem)
        if m := re.match(r"proc\s+(\w+)", low):
            f["procs"].append(m.group(1).upper())
            in_sql = m.group(1) == "sql"
        if low == "quit" or (low == "run" and not in_sql):
            in_sql = False if low == "quit" else in_sql
        # user-macro calls (resolved against the global registry later)
        for c in re.finditer(r"%(\w+)\s*(\(([^;]*)\))?", s):
            name = c.group(1).lower()
            if cur_macro:
                f["macros_defined"][cur_macro].add(name)
            else:
                f["open_calls"].append(name)
            if c.group(3):
                f["call_args"].append((name, c.group(3), cur_macro is not None))
        f["into_vars"] += re.findall(r"\binto\s*:\s*(\w+)", low)
        if low.startswith("%"):  # macro statements/calls: tables come from call arguments instead
            continue
        # table references
        if re.match(r"data\s+(?!=)", low) and not low.startswith("data _null_"):
            f["n_data_steps"] += 1
            step_writes = re.findall(TABLE, re.sub(r"\([^)]*\)", "", s[5:]))
            for t in step_writes:
                f["writes"].add(t)
        if m := re.match(r"(set|merge)\s+(.*)", s, re.IGNORECASE):
            f["reads"].update(re.findall(TABLE, re.sub(r"\([^)]*\)", "", m.group(2))))
        for t in re.findall(r"\bdata\s*=\s*" + TABLE, s, re.IGNORECASE):
            f["reads"].add(t)
        for t in re.findall(r"\b(?:out|outest)\s*=\s*" + TABLE, s, re.IGNORECASE):
            f["writes"].add(t)
        for t in re.findall(r"\bcreate\s+table\s+" + TABLE, s, re.IGNORECASE):
            f["writes"].add(t)
        for t in re.findall(r"\b(?:from|join)\s+" + TABLE, s, re.IGNORECASE):
            f["reads"].add(t)
        f["n_joins"] += len(re.findall(r"\bjoin\b", low)) + (1 if low.startswith("merge") else 0)
    f["has_retain"] = bool(
        re.search(r"\bretain\b|(^|;)\s*\w+\s*\+\s*[\w.]+\s*;", code, re.IGNORECASE | re.MULTILINE)
    )
    f["has_first_last"] = bool(re.search(r"\b(first|last)\.\w+", code, re.IGNORECASE))
    f["n_do_loops"] = len(re.findall(r"%do\b", code, re.IGNORECASE))
    for fact, pattern in PATTERN_FACTS.items():
        f[fact] = int(bool(re.search(pattern, code, re.IGNORECASE)))
    return f


def macro_depth(name, registry, seen=()):
    """Nesting depth of a macro: 1 + the deepest user macro it calls (0 if unknown; cycles are cut)."""
    if name not in registry or name in seen:
        return 0
    inner = [c for c in registry[name] if c in registry]
    return 1 + max((macro_depth(c, registry, seen + (name,)) for c in inner), default=0)


def analyze(prog_dir=PROG_DIR, log_dir=LOG_DIR):
    """Parse every .sas file in prog_dir; return (inventory, dependencies, nodes, edges)."""
    progs = [parse_program(p) for p in sorted(Path(prog_dir).glob("*.sas"))]
    # pass 1: every macro defined in ANY program (a macro can be defined in 06 and called in 07),
    # so that %name( counts as a call only for real user macros, not %let, %do, %scan, ...
    registry, defined_in = {}, {}
    for p in progs:
        for m, calls in p["macros_defined"].items():
            registry[m], defined_in[m] = calls, p["program_id"]

    inv, deps = [], []

    def dep(pid, obj, otype, rel, conf, ev):
        """Record one dependency row (program -> object) with its confidence and the evidence for it."""
        deps.append(
            {
                "program_id": pid,
                "object": obj.lower(),
                "object_type": otype,
                "relationship": rel,
                "confidence": conf,
                "evidence": ev,
            }
        )

    for p in progs:
        pid = p["program_id"]
        user_calls = [c for c in p["open_calls"] if c in registry]
        called_anywhere = set(user_calls) | {
            c for m in p["macros_defined"].values() for c in m if c in registry
        }
        # tables from user-macro call arguments: key "out" = written, other lib.table values = read
        for name, args, inside_def in p["call_args"]:
            if name not in registry:
                continue
            for k, v in re.findall(r"(\w+)\s*=\s*([^,]+)", args):
                v = v.strip()
                if re.fullmatch(TABLE, v) and is_table(v):
                    rel = "WRITES" if k.lower() == "out" else "READS"
                    conf = "LOW" if "&" in v else "MEDIUM"
                    dep(pid, v, "TABLE", rel, conf, f"argument {k}= of %{name}")
        # tables written literally in the code: HIGH, or LOW when the name contains &var
        for kind in ("reads", "writes"):
            for t in p[kind]:
                if not is_table(t):
                    continue
                if "&" in t:
                    p["dynamic"].add(t)
                    if t.split(".")[0].startswith("&") or "&" in t:
                        dep(pid, t, "TABLE", kind.upper(), "LOW", "static (unresolved &var)")
                else:
                    dep(pid, t, "TABLE", kind.upper(), "HIGH", "static")
        # MPRINT: resolved code written by SAS while the macros ran
        mp = Path(log_dir) / f"{pid}_mprint.sas"
        if mp.exists():
            r = parse_program(mp)
            for kind in ("reads", "writes"):
                for t in r[kind]:
                    if is_table(t) and "&" not in t:
                        dep(pid, t, "TABLE", kind.upper(), "MEDIUM", "mprint")
        for file in sorted(p["files_read"]):  # source files: where the data lineage starts
            dep(pid, file, "FILE", "READS", "HIGH", "static (infile)")
        for lib in p["libraries"]:
            dep(pid, lib, "LIBRARY", "USES_LIBRARY", "HIGH", "static")
        for m in p["macros_defined"]:
            dep(pid, m, "MACRO", "DEFINES", "HIGH", "static")
        for m in sorted(called_anywhere):
            if defined_in.get(m) != pid or m in user_calls:
                dep(pid, m, "MACRO", "CALLS", "HIGH", "static")
        for inc in p["includes"]:
            dep(pid, inc, "PROGRAM", "INCLUDES", "HIGH", "static")

        procs = p["procs"]
        inv.append(
            {
                "program_id": pid,
                "program_name": p["program_name"],
                "path": p["path"],
                "size_bytes": p["size_bytes"],
                "loc": p["loc"],
                "n_data_steps": p["n_data_steps"],
                "n_procs": len(procs),
                "procs": "|".join(sorted(set(procs))),
                "n_macros_defined": len(p["macros_defined"]),
                "n_macro_calls": len(user_calls),
                "max_macro_nesting": max((macro_depth(c, registry) for c in user_calls), default=0),
                "n_includes": len(p["includes"]),
                "n_into": len(p["into_vars"]),
                "n_do_loops": p["n_do_loops"],
                "n_joins": p["n_joins"],
                "has_retain": int(p["has_retain"]),
                "has_first_last": int(p["has_first_last"]),
                "has_format": int("FORMAT" in procs),
                "has_reg": int("REG" in procs),
                "has_forecast": int("FORECAST" in procs),
                "n_dynamic_names": len(p["dynamic"]),
                **{fact: p[fact] for fact in PATTERN_FACTS},
                "libraries": "|".join(sorted(p["libraries"])),
                "analyzer_version": ANALYZER_VERSION,
            }
        )

    deps = pd.DataFrame(deps).drop_duplicates(["program_id", "object", "relationship", "evidence"])
    inv = pd.DataFrame(inv)
    io = deps[deps.object_type == "TABLE"]
    inv["n_inputs"] = (
        inv.program_id.map(io[io.relationship == "READS"].groupby("program_id").object.nunique())
        .fillna(0)
        .astype(int)
    )
    low_tbl = io[io.confidence == "LOW"].groupby("program_id").object.nunique()
    inv["n_dynamic_names"] = inv.program_id.map(low_tbl).fillna(0).astype(int)
    inv["n_outputs"] = (
        inv.program_id.map(io[io.relationship == "WRITES"].groupby("program_id").object.nunique())
        .fillna(0)
        .astype(int)
    )
    # programs whose output tables this program reads (cross-program dependencies)
    written_by = io[io.relationship == "WRITES"][["object", "program_id"]].rename(
        columns={"program_id": "writer"}
    )
    reads = io[io.relationship == "READS"][["program_id", "object"]].merge(written_by, on="object")
    upstream = reads[reads.writer != reads.program_id].groupby("program_id").writer.nunique()
    inv["n_upstream_programs"] = inv.program_id.map(upstream).fillna(0).astype(int)

    # lineage graph: nodes + edges in flow direction
    nodes, edges = {}, []
    for pid in inv.program_id:
        nodes[pid] = {"node_id": pid, "node_type": "PROGRAM", "node_name": pid}
    for d in deps.itertuples():
        nid = d.object
        nodes.setdefault(nid, {"node_id": nid, "node_type": d.object_type, "node_name": nid})
        src, tgt = {
            "READS": (nid, d.program_id),
            "WRITES": (d.program_id, nid),
            "USES_LIBRARY": (nid, d.program_id),
            "DEFINES": (d.program_id, nid),
            "CALLS": (nid, d.program_id),
            "INCLUDES": (nid, d.program_id),
        }[d.relationship]
        edges.append(
            {
                "source_id": src,
                "target_id": tgt,
                "relationship": d.relationship,
                "confidence": d.confidence,
                "evidence": d.evidence,
            }
        )
    # file -> table: which source file each DATA step loads (data lineage down to the raw files)
    for p in progs:
        for file, table in sorted(p["file_loads"]):
            if is_table(table):
                edges.append({"source_id": file, "target_id": table.lower(), "relationship": "LOADS",
                              "confidence": "HIGH", "evidence": f"infile in the data step of {p['program_id']}"})  # fmt: skip
    # nested macros: a macro calling another macro (code lineage between macros, e.g. run_period -> summarize)
    for m, calls in registry.items():
        for c in sorted(calls & set(registry)):
            edges.append({"source_id": c, "target_id": m, "relationship": "CALLS", "confidence": "HIGH",
                          "evidence": f"called inside %{m}"})  # fmt: skip
    edges = pd.DataFrame(edges)
    # the same edge can be found several ways (e.g. static and MPRINT): keep the most confident one
    rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    edges = (
        edges.assign(r=edges.confidence.map(rank))
        .sort_values("r")
        .drop_duplicates(["source_id", "target_id", "relationship"])
        .drop(columns="r")
    )
    nodes = pd.DataFrame(nodes.values())
    nodes["library"] = nodes.apply(
        lambda n: n.node_id.split(".")[0] if n.node_type == "TABLE" else "", axis=1
    )

    # sorted rows: the same code always gives byte-identical files (sets have no stable order)
    deps = deps.sort_values(["program_id", "relationship", "object", "evidence"], ignore_index=True)
    edges = edges.sort_values(["source_id", "target_id", "relationship"], ignore_index=True)
    nodes = nodes.sort_values(["node_type", "node_id"], ignore_index=True)
    return inv, deps, nodes, edges


def main():
    """Analyze sas/programs and write the four CSV files to outputs/."""
    OUT_DIR.mkdir(exist_ok=True)
    inv, deps, nodes, edges = analyze()
    inv.to_csv(OUT_DIR / "migration_inventory.csv", index=False)
    deps.to_csv(OUT_DIR / "program_dependencies.csv", index=False)
    nodes.to_csv(OUT_DIR / "lineage_nodes.csv", index=False)
    edges.to_csv(OUT_DIR / "lineage_edges.csv", index=False)
    print(
        inv[
            [
                "program_id",
                "loc",
                "procs",
                "n_inputs",
                "n_outputs",
                "n_macros_defined",
                "max_macro_nesting",
                "n_includes",
                "n_into",
                "n_dynamic_names",
            ]
        ].to_string(index=False)
    )
    print(f"\n{len(nodes)} nodes, {len(edges)} edges -> {OUT_DIR}")


if __name__ == "__main__":
    main()
