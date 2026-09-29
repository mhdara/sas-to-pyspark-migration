"""lineage_trace.py - back-trace any table, program or macro to everything it depends on, down to the source files.

Walks the analyzer's lineage graph (outputs/lineage_edges.csv) against the direction of the arrows: a table is
written by a program, the program reads tables, includes programs and calls macros, the macros are defined in
programs, ... until the raw CSV files. Data lineage (tables) and code lineage (programs, %include, macros) are
followed together, because in SAS a program can depend on another one with no data in between.

Run:  python python/lineage_trace.py out.exec_report      print the upstream tree of one node
      python python/lineage_trace.py                      write outputs/lineage_paths.csv for every out table
Out:  outputs/lineage_paths.csv  one row per (output table, upstream node): depth, relationship, evidence
"""

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
VERB = {  # how an upstream edge reads when walking backwards from its target
    "WRITES": "written by",
    "READS": "reads",
    "INCLUDES": "includes",
    "CALLS": "calls macro",
    "DEFINES": "defined in",
    "LOADS": "loaded from",
}


def load_edges() -> pd.DataFrame:
    """Lineage edges without library links, and without a program reading a table it writes itself (a loop)."""
    e = pd.read_csv(ROOT / "outputs" / "lineage_edges.csv")
    e = e[e.relationship != "USES_LIBRARY"]
    files = set(e.source_id[e.relationship == "LOADS"])  # a file is traced through the table it loads
    e = e[~((e.relationship == "READS") & e.source_id.isin(files))]
    written = set(zip(e[e.relationship == "WRITES"].source_id, e[e.relationship == "WRITES"].target_id))
    own = e.apply(lambda r: r.relationship == "READS" and (r.target_id, r.source_id) in written, axis=1)
    return e[~own]


def node_types() -> dict[str, str]:
    """node id -> TABLE / PROGRAM / MACRO / FILE / LIBRARY."""
    n = pd.read_csv(ROOT / "outputs" / "lineage_nodes.csv")
    return dict(zip(n.node_id, n.node_type))


def upstream_edges(e: pd.DataFrame, node: str) -> list:
    """Edges arriving at a node; a macro is traced to the program that defines it (DEFINES points program -> macro)."""
    return sorted(e[e.target_id == node].itertuples(index=False), key=lambda r: (r.relationship, r.source_id))


def tree(node: str, e: pd.DataFrame, types: dict, prefix: str = "", seen: set | None = None) -> list[str]:
    """Indented upstream tree; a node already expanded is marked instead of repeated."""
    seen = set() if seen is None else seen
    lines = []
    edges = upstream_edges(e, node)
    for i, r in enumerate(edges):
        last = i == len(edges) - 1
        mark = "(see above)" if r.source_id in seen else ""
        lines.append(
            f"{prefix}{'└─' if last else '├─'} {VERB[r.relationship]} {r.source_id} "
            f"[{types.get(r.source_id, '?').lower()}, {r.confidence}] {mark}".rstrip()
        )
        if not mark:
            seen.add(r.source_id)
            lines += tree(r.source_id, e, types, prefix + ("   " if last else "│  "), seen)
    return lines


def paths(e: pd.DataFrame, types: dict) -> pd.DataFrame:
    """For every out table: each upstream node once, at its shortest distance, with the edge that reaches it."""
    rows = []
    for target in sorted(
        t for t, k in types.items() if k == "TABLE" and t.startswith("out.") and "&" not in t
    ):
        frontier, depth, seen = [target], 0, {target}
        while frontier:
            depth += 1
            nxt = []
            for node in frontier:
                for r in upstream_edges(e, node):
                    if r.source_id in seen:
                        continue
                    seen.add(r.source_id)
                    nxt.append(r.source_id)
                    rows.append(
                        {
                            "target_id": target,
                            "upstream_id": r.source_id,
                            "upstream_type": types.get(r.source_id, ""),
                            "depth": depth,
                            "relationship": r.relationship,
                            "via": node,
                            "confidence": r.confidence,
                            "evidence": r.evidence,
                        }
                    )
            frontier = nxt
    return pd.DataFrame(rows)


def main():
    """Print one node's upstream tree, or write the upstream paths of every out table."""
    e, types = load_edges(), node_types()
    if len(sys.argv) > 1:
        node = sys.argv[1].lower()
        print(f"{node} [{types.get(node, '?').lower()}]")
        print("\n".join(tree(node, e, types)))
        return
    df = paths(e, types)
    df.to_csv(ROOT / "outputs" / "lineage_paths.csv", index=False)
    print(f"{len(df)} upstream rows for {df.target_id.nunique()} output tables -> outputs/lineage_paths.csv")


if __name__ == "__main__":
    main()
