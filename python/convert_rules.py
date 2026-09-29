"""convert_rules.py - RULE-BASED code migration: SAS -> PySpark with fixed templates, no LLM.

The third conversion method, deterministic like the rule-based transpilers sold for migrations. It
translates a SUBSET of SAS; a program using anything outside it is marked NOT_SUPPORTED with the
reason, never guessed. Supported:
  DATA step    one SET input, assignments (strip, upcase, lowcase, missing, put with a user format,
               literals, columns), IF / ELSE IF / ELSE chains assigning one column, LENGTH/FORMAT
               (metadata, ignored), KEEP / DROP
  PROC SQL     CREATE TABLE ... AS SELECT (SAS-only syntax rewritten: CALCULATED, INTNX month
               start, column FORMAT=); not SELECT ... INTO (macro variables)
  PROC SORT    BY, OUT=, NODUPKEY (keeps the first row of each key, in input order)
  PROC MEANS   NWAY, CLASS, VAR (one), OUTPUT OUT= with N / SUM / MEAN / MIN / MAX / STD
  PROC FREQ    TABLES a*b / OUT= (count and percent, missing levels excluded)
  PROC FORMAT  VALUE with low / high / -< / <- ranges and OTHER
Not supported (by design): macros (%macro, %let, %include, &var), CALL SYMPUT, SELECT INTO, RETAIN,
sum statements, FIRST./LAST., MERGE, arrays, PROC REG, PROC FORECAST and any other procedure.

Run:  python python/convert_rules.py [--run 1] [program_id ...]
Out:  converted/rule_based/run<k>/p<id>.py, then the same run + validation as the LLM methods;
      outputs/conversion/{attempts,status,validation}_rule_based_run<k>.csv
"""

import argparse
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from analyzer import strip_comments
from convert import MAP, facts_of, post_checks, run_sandboxed
from validate import py_out_dir, validate_program

ROOT = Path(__file__).resolve().parents[1]
METHOD = "rule_based"
FUNCS = {"strip": "F.trim({})", "upcase": "F.upper({})", "lowcase": "F.lower({})"}
COMPARE = {"<": "<", "lt": "<", "<=": "<=", "le": "<=", ">": ">", "gt": ">", ">=": ">=", "ge": ">=",
           "=": "==", "eq": "==", "ne": "!=", "^=": "!="}  # fmt: skip
STATS = {"n": "count", "sum": "sum", "mean": "avg", "min": "min", "max": "max", "std": "stddev"}


class Unsupported(Exception):
    """A SAS construct outside the supported subset: the program is NOT_SUPPORTED."""


# ---------------------------------------------------------------- expressions
def literal(text: str) -> str | None:
    """Python/Spark literal for a SAS string or number, or None."""
    if m := re.fullmatch(r"'([^']*)'|\"([^\"]*)\"", text):
        return f"F.lit({(m.group(1) if m.group(1) is not None else m.group(2))!r})"
    if re.fullmatch(r"-?\d+(\.\d+)?", text):
        return f"F.lit({text})"
    return None


def expr(text: str, formats: dict) -> str:
    """Translate a SAS expression of the supported subset into a Spark column expression."""
    text = text.strip()
    if (lit := literal(text)) is not None:
        return lit
    if re.fullmatch(r"[A-Za-z_]\w*", text):
        return f'F.col("{text.lower()}")'
    if m := re.fullmatch(r"(\w+)\s*\((.*)\)", text, re.DOTALL):
        name, arg = m.group(1).lower(), m.group(2).strip()
        if name in FUNCS:
            return FUNCS[name].format(expr(arg, formats))
        if name == "missing":
            return f"{expr(arg, formats)}.isNull().cast('int')"
        if (
            name == "put"
            and (p := re.fullmatch(r"(\w+)\s*,\s*(\w+)\.", arg))
            and p.group(2).lower() in formats
        ):
            return formats[p.group(2).lower()](f'F.col("{p.group(1).lower()}")')
    raise Unsupported(f"expression '{text}'")


def condition(text: str) -> str:
    """One comparison 'column op number'. SAS: a missing number is smaller than any number, so
    '<' and '<=' are TRUE for missing values (Spark would give null/false)."""
    m = re.fullmatch(
        r"(\w+)\s*(<=|>=|\^=|<|>|=|lt|le|gt|ge|eq|ne)\s*(-?\d+(?:\.\d+)?)", text.strip(), re.IGNORECASE
    )
    if not m:
        raise Unsupported(f"condition '{text.strip()}'")
    col, op, num = f'F.col("{m.group(1).lower()}")', COMPARE[m.group(2).lower()], m.group(3)
    test = f"({col} {op} {num})"
    return f"({col}.isNull() | {test})" if op in ("<", "<=") else test


def format_function(body: str):
    """Build a translator for a PROC FORMAT VALUE: ranges -> an F.when chain (OTHER gets missing too)."""
    branches, other = [], None
    for rng, label in re.findall(r"(.+?)\s*=\s*'([^']*)'", body):
        rng = rng.strip().lower()
        if rng == "other":
            other = label
            continue
        m = re.fullmatch(r"(low|-?\d+(?:\.\d+)?)\s*(-<|<-|-)\s*(high|-?\d+(?:\.\d+)?)", rng)
        if not m:
            raise Unsupported(f"format range '{rng}'")
        lo, op, hi = m.groups()
        parts = [] if lo == "low" else [f"(c {'>' if op == '<-' else '>='} {lo})"]
        parts += [] if hi == "high" else [f"(c {'<' if op == '-<' else '<='} {hi})"]
        branches.append((" & ".join(parts) or "F.lit(True)", label))

    def apply(col: str) -> str:
        chain = "F"
        for cond, label in branches:  # SAS: low/high never match a missing value
            chain += f".when({col}.isNotNull() & {cond.replace('c ', col + ' ')}, F.lit({label!r}))"
        return chain + (f".otherwise(F.lit({other!r}))" if other is not None else ".otherwise(F.lit(None))")

    return apply


# ---------------------------------------------------------------- blocks
def table_ref(name: str) -> tuple[str, str]:
    """(library, table) for lib.table or a one-level WORK name."""
    lib, _, table = name.lower().rpartition(".")
    return (lib or "work"), table


def read_code(name: str) -> str:
    lib, table = table_ref(name)
    return f'read("{lib}", "{table}")'


def save_code(var: str, name: str) -> str:
    lib, table = table_ref(name)
    return f'save({var}, "{lib}", "{table}")'


def options(text: str) -> dict:
    """KEY=value options of a PROC statement (lower-case keys)."""
    return {k.lower(): v for k, v in re.findall(r"(\w+)\s*=\s*([\w.&]+(?:\([^)]*\))?)", text)}


def data_step(stmts: list[str], formats: dict) -> list[str]:
    """DATA out; SET in; assignments; IF/ELSE chains; KEEP/DROP; RUN."""
    header = re.fullmatch(r"data\s+([\w.]+)", stmts[0], re.IGNORECASE)
    if not header:
        raise Unsupported(f"DATA statement '{stmts[0]}' (several outputs or dataset options)")
    lines, i, src = [], 1, None
    while i < len(stmts):
        s, low = stmts[i], stmts[i].lower()
        if m := re.fullmatch(r"set\s+([\w.]+)", s, re.IGNORECASE):
            src = m.group(1)
            lines.append(f"df = {read_code(src)}")
        elif low.startswith(("length ", "format ", "label ", "informat ")):
            pass  # metadata: no effect on values here
        elif m := re.fullmatch(r"(keep|drop)\s+(.+)", s, re.IGNORECASE):
            cols = [f'"{c.lower()}"' for c in m.group(2).split()]
            lines.append(f"df = df.{'select' if m.group(1).lower() == 'keep' else 'drop'}({', '.join(cols)})")
        elif low.startswith("if "):
            chain, target = "F", None
            while i < len(stmts) and re.match(r"(else\s+)?if\s", stmts[i], re.IGNORECASE):
                m = re.fullmatch(
                    r"(?:else\s+)?if\s+(.+?)\s+then\s+(\w+)\s*=\s*(.+)", stmts[i], re.IGNORECASE | re.DOTALL
                )
                if not m or (target and m.group(2).lower() != target):
                    raise Unsupported(f"IF statement '{stmts[i]}'")
                target = m.group(2).lower()
                chain += f".when({condition(m.group(1))}, {expr(m.group(3), formats)})"
                i += 1
            if i < len(stmts) and (
                m := re.fullmatch(r"else\s+(\w+)\s*=\s*(.+)", stmts[i], re.IGNORECASE | re.DOTALL)
            ):
                chain += f".otherwise({expr(m.group(2), formats)})"
            else:
                i -= 1
                # no ELSE: SAS keeps the column's current value (missing if the column is new)
                chain += f'.otherwise(F.col("{target}") if "{target}" in df.columns else F.lit(None))'
            lines.append(f'df = df.withColumn("{target}", {chain})')
        elif m := re.fullmatch(r"(\w+)\s*=\s*(.+)", s, re.DOTALL):
            lines.append(f'df = df.withColumn("{m.group(1).lower()}", {expr(m.group(2), formats)})')
        else:
            raise Unsupported(f"DATA step statement '{s.split()[0]}'")
        i += 1
    if src is None:
        raise Unsupported("DATA step without a single SET input")
    return [*lines, save_code("df", header.group(1))]


def sql_step(stmts: list[str]) -> list[str]:
    """PROC SQL: each CREATE TABLE ... AS SELECT becomes spark.sql() on temporary views."""
    lines = []
    for s in stmts[1:]:
        if re.search(r"\binto\s*:", s, re.IGNORECASE):
            raise Unsupported("SELECT ... INTO (macro variable)")
        m = re.fullmatch(r"create\s+table\s+([\w.]+)\s+as\s+(select\s.+)", s, re.IGNORECASE | re.DOTALL)
        if not m:
            raise Unsupported(f"PROC SQL statement '{s[:40]}'")
        query = re.sub(r"\bcalculated\s+", "", m.group(2), flags=re.IGNORECASE)
        query = re.sub(
            r"intnx\s*\(\s*'month'\s*,\s*([\w.]+)\s*,\s*0\s*,\s*'b'\s*\)",
            r"trunc(\1, 'MM')",
            query,
            flags=re.IGNORECASE,
        )
        query = re.sub(r"\s+format\s*=\s*\w+\.", "", query, flags=re.IGNORECASE)
        for name in sorted(set(re.findall(r"\b(?:from|join)\s+([\w.]+)", query, re.IGNORECASE))):
            lib, table = table_ref(name)
            lines.append(f'{read_code(name)}.createOrReplaceTempView("{lib}__{table}")')
            query = re.sub(rf"\b{re.escape(name)}\b", f"{lib}__{table}", query, flags=re.IGNORECASE)
        lines += [f"df = spark.sql({query!r})", save_code("df", m.group(1))]
    return lines


def sort_step(stmts: list[str]) -> list[str]:
    """PROC SORT DATA= OUT= [NODUPKEY]; BY keys. NODUPKEY keeps the first row of each key in input order."""
    opt, by = options(stmts[0]), None
    for s in stmts[1:]:
        if m := re.fullmatch(r"by\s+(.+)", s, re.IGNORECASE):
            by = [f'"{c.lower()}"' for c in m.group(1).split()]
        else:
            raise Unsupported(f"PROC SORT statement '{s}'")
    if not by or "data" not in opt:
        raise Unsupported("PROC SORT without DATA= or BY")
    keys = ", ".join(by)
    lines = [f"df = {read_code(opt['data'])}"]
    if re.search(r"\bnodupkey\b", stmts[0], re.IGNORECASE):
        lines += [
            'df = df.withColumn("_order", F.monotonically_increasing_id())  # input order',
            f'w = Window.partitionBy({keys}).orderBy("_order")',
            'df = df.withColumn("_n", F.row_number().over(w)).filter("_n = 1").drop("_n", "_order")',
        ]
    elif re.search(r"\bdup", stmts[0], re.IGNORECASE):
        raise Unsupported("PROC SORT duplicate option other than NODUPKEY")
    return [*lines, save_code("df", opt.get("out", opt["data"]))]


def means_step(stmts: list[str]) -> list[str]:
    """PROC MEANS/SUMMARY NWAY: CLASS, one VAR, OUTPUT OUT= with named statistics."""
    head = stmts[0]
    if not re.search(r"\bnway\b", head, re.IGNORECASE):
        raise Unsupported("PROC MEANS without NWAY (all class combinations)")
    data = options(head).get("data")
    cls = var = out = None
    stats = []
    for s in stmts[1:]:
        if m := re.fullmatch(r"class\s+(.+)", s, re.IGNORECASE):
            cls = [c.lower() for c in m.group(1).split()]
        elif m := re.fullmatch(r"var\s+(\w+)", s, re.IGNORECASE):
            var = m.group(1).lower()
        elif m := re.fullmatch(
            r"output\s+out\s*=\s*([\w.]+)(\([^)]*\))?\s+(.+)", s, re.IGNORECASE | re.DOTALL
        ):
            out, dsopt = m.group(1), (m.group(2) or "").lower()
            if dsopt and not re.fullmatch(r"\(\s*drop\s*=\s*_type_\s+_freq_\s*\)", dsopt):
                raise Unsupported(f"PROC MEANS output option {dsopt}")
            stats = [(k.lower(), v.lower()) for k, v in re.findall(r"(\w+)\s*=\s*(\w+)", m.group(3))]
            keep_type_freq = not dsopt
        else:
            raise Unsupported(f"PROC MEANS statement '{s.split()[0]}'")
    if not (data and cls and var and out and stats) or any(k not in STATS for k, _ in stats):
        raise Unsupported("PROC MEANS form")
    aggs = [f'F.{STATS[k]}("{var}").alias("{name}")' for k, name in stats]
    if keep_type_freq:
        aggs = [f'F.lit({2 ** len(cls) - 1}).alias("_type_")', 'F.count(F.lit(1)).alias("_freq_")', *aggs]
    group = ", ".join(f'"{c}"' for c in cls)
    return [f"df = {read_code(data)}.groupBy({group}).agg({', '.join(aggs)})", save_code("df", out)]


def freq_step(stmts: list[str]) -> list[str]:
    """PROC FREQ; TABLES a*b / OUT=: counts and percents (rows with a missing level excluded)."""
    data = options(stmts[0]).get("data")
    lines = []
    for s in stmts[1:]:
        m = re.fullmatch(r"tables\s+([\w*]+)\s*/\s*out\s*=\s*([\w.]+)", s, re.IGNORECASE)
        if not m or not data:
            raise Unsupported(f"PROC FREQ statement '{s}'")
        cols = [c.lower() for c in m.group(1).split("*")]
        group = ", ".join(f'"{c}"' for c in cols)
        lines += [
            f"df = {read_code(data)}.dropna(subset=[{group}])",
            "total = df.count()",
            (
                f'df = df.groupBy({group}).agg(F.count(F.lit(1)).alias("count"))'
                '.withColumn("percent", F.col("count") * 100.0 / total)'
            ),
            save_code("df", m.group(2)),
        ]
    return lines


# ---------------------------------------------------------------- program
HEADER = '''"""p{pid}.py - generated by convert_rules.py (rule-based, deterministic) from {source}."""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").appName("{pid}").getOrCreate()
LIB = {libs!r}
work = {{}}  # SAS WORK tables live in memory


def read(lib, table):
    """A table of a SAS library: migrated Parquet (ctrl, stg), a converted output (out) or WORK."""
    if lib == "work":
        return work[table]
    path = f"{{LIB[lib]}}/{{table}}" + (".parquet" if lib in ("ctrl", "stg") else "")
    return spark.read.parquet(path)


def save(df, lib, table):
    """Write an `out` table as Parquet; keep a WORK table in memory."""
    if lib == "work":
        work[table] = df
    else:
        df.write.mode("overwrite").parquet(f"{{LIB[lib]}}/{{table}}")
'''


def translate(code: str) -> list[str]:
    """Translate a whole program; raise Unsupported at the first construct outside the subset."""
    stmts = [re.sub(r"\s+", " ", s).strip() for s in strip_comments(code).split(";") if s.strip()]
    setup = re.compile(
        r"(libname|filename|options|title|footnote)\b", re.IGNORECASE
    )  # environment, not logic
    if any(re.search(r"%\w+|&\w+", s) for s in stmts if not setup.match(s)):
        raise Unsupported("macro language (%macro, %let, %include, &var)")
    for pattern, what in [(r"\bretain\b", "RETAIN"), (r"\b(first|last)\.\w+", "FIRST./LAST."),
                          (r"\bmerge\b", "MERGE"), (r"\barray\b", "arrays"),
                          (r"\bcall\s+\w+", "CALL routines")]:  # fmt: skip
        if re.search(pattern, " ; ".join(stmts), re.IGNORECASE):
            raise Unsupported(what)
    lines, formats, i = [], {}, 0
    while i < len(stmts):
        s = stmts[i]
        if setup.match(s):
            i += 1
            continue
        end = "quit" if re.match(r"proc\s+sql\b", s, re.IGNORECASE) else "run"
        j = next((k for k in range(i + 1, len(stmts)) if stmts[k].lower() in (end, "quit", "run")), None)
        if j is None:
            raise Unsupported(f"block '{s[:30]}' without RUN/QUIT")
        block = stmts[i:j]
        kind = s.split()[0].lower() + ("" if s.lower().startswith("data") else " " + s.split()[1].lower())
        lines.append(f"\n# ---- {s[:70]}")
        if kind == "data":
            lines += data_step(block, formats)
        elif kind == "proc sql":
            lines += sql_step(block)
        elif kind == "proc sort":
            lines += sort_step(block)
        elif kind in ("proc means", "proc summary"):
            lines += means_step(block)
        elif kind == "proc freq":
            lines += freq_step(block)
        elif kind == "proc format":
            for v in block[1:]:
                m = re.fullmatch(r"value\s+(\w+)\s+(.+)", v, re.IGNORECASE | re.DOTALL)
                if not m:
                    raise Unsupported(f"PROC FORMAT statement '{v[:30]}'")
                formats[m.group(1).lower()] = format_function(m.group(2))
            lines.append("# (format translated into the expressions that use it)")
        else:
            raise Unsupported(f"{kind.upper()}")
        i = j + 1
    return lines


def main():
    """Translate each program, then run and validate it like the LLM methods (no repair: rules are fixed)."""
    parser = argparse.ArgumentParser(description="rule-based SAS -> PySpark conversion")
    parser.add_argument("--run", type=int, default=1)
    parser.add_argument("programs", nargs="*")
    args = parser.parse_args()
    conv_dir, out_dir = ROOT / "converted" / METHOD / f"run{args.run}", py_out_dir(METHOD, args.run)
    results_dir = ROOT / "outputs" / "conversion"
    for folder in (conv_dir, out_dir, results_dir):
        folder.mkdir(parents=True, exist_ok=True)
    libs = {"ctrl": MAP["ctrl"], "stg": MAP["stg"], "out": str(out_dir.relative_to(ROOT))}
    inv = pd.read_csv(ROOT / "outputs" / "migration_inventory.csv")
    deps = pd.read_csv(ROOT / "outputs" / "program_dependencies.csv")
    only = set(args.programs)
    attempts, status, checks = [], [], []
    for pid, rel_path in zip(inv.program_id, inv.path, strict=True):
        if (only and pid not in only) or pid == "00_load_raw":
            continue
        start, facts = datetime.now(UTC), facts_of(pid, deps)
        try:
            body = translate((ROOT / rel_path).read_text(encoding="utf-8"))
        except Unsupported as reason:
            status.append({"program_id": pid, "final_status": "NOT_SUPPORTED", "reason": str(reason),
                           "attempts": 0, "method": METHOD, "run": args.run})  # fmt: skip
            print(f"{pid}: NOT_SUPPORTED ({reason})")
            continue
        target = conv_dir / f"p{pid}.py"
        target.write_text(HEADER.format(pid=pid, source=rel_path, libs=libs) + "\n".join(body) + "\n")
        for t in facts["outputs"]:
            shutil.rmtree(out_dir / t.split(".", 1)[1], ignore_errors=True)
        problems = post_checks(target, facts, (ROOT / rel_path).read_text(encoding="utf-8"))
        ok_exec, err = (False, "; ".join(problems)) if problems else run_sandboxed(target, conv_dir)
        result = validate_program(pid, out_dir) if ok_exec else []
        failed = [c for c in result if not c["passed"]]
        ok_val = ok_exec and not failed
        seconds = round((datetime.now(UTC) - start).total_seconds(), 2)
        attempts.append({"program_id": pid, "method": METHOD, "run": args.run, "attempt": 1,
                         "post_checks_ok": int(not problems), "execution_ok": int(ok_exec),
                         "validation_ok": int(ok_val), "n_checks": len(result), "n_failed_checks": len(failed),
                         "error": err[-500:].replace("\n", " ") if not ok_exec else "", "model": "rules",
                         "latency_s": seconds, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0,
                         "prompt_version": "", "timestamp": start.isoformat(timespec="seconds")})  # fmt: skip
        checks += [c | {"method": METHOD, "run": args.run} for c in result]
        status.append({"program_id": pid, "final_status": "VALIDATED" if ok_val else "MANUAL_REVIEW",
                       "reason": "" if ok_val else ("execution_error" if not ok_exec else "validation_difference"),
                       "attempts": 1, "method": METHOD, "run": args.run})  # fmt: skip
        print(f"{pid}: exec={ok_exec} validation={ok_val} failed_checks={len(failed)}")
    tag = f"{METHOD}_run{args.run}"
    for name, rows in (("attempts", attempts), ("status", status), ("validation", checks)):
        path, new = results_dir / f"{name}_{tag}.csv", pd.DataFrame(rows)
        if only and path.exists():
            old = pd.read_csv(path)
            new = pd.concat([old[~old.program_id.isin(only)], new], ignore_index=True)
        new.to_csv(path, index=False)


if __name__ == "__main__":
    main()
