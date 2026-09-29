"""Rules v2 on small SAS snippets: one scenario per rule, including patterns the 12 case programs
do not contain. Run from the project root: pytest -q"""

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
from analyzer import analyze
from classify_rules_v2 import RULES, classify

SNIPPETS = {
    "logistic_pd": """
        proc logistic data=risk.loan_book outest=risk.pd_model;
          model default_flag = ltv income;
        run;""",
    "arima": """
        proc arima data=stg.monthly_volumes;
          identify var=volume; estimate p=1; forecast lead=12 out=out.volume_forecast;
        run;""",
    "call_execute": """
        data _null_;
          set ctrl.jobs;
          call execute(cats('%run_job(', job_id, ');'));
        run;""",
    "macro_only": """
        %macro clean(ds=); data &ds; set &ds; run; %mend clean;""",
    "proc_report": """
        proc report data=out.branch_kpi; column branch kpi; run;""",
    "ods_excel": """
        ods excel file="/reports/monthly.xlsx";
        proc means data=out.balances; var balance; run;
        ods excel close;""",
    "hash_lookup": """
        data out.enriched;
          if _n_ = 1 then do; declare hash h(dataset: 'stg.rates'); h.definekey('code'); h.definedone(); end;
          set stg.accounts; rc = h.find();
        run;""",
    "passthrough": """
        proc sql;
          connect to oracle (path=prod);
          create table out.card_txn as select * from connection to oracle (select * from txn);
          disconnect from oracle;
        quit;""",
    "raw_load": """
        data stg.branches; infile "/raw/branches.csv" dsd firstobs=2; input branch_id name $; run;""",
    "region_sales": """
        data out.region_sales; set stg.region_totals; run;""",
    "card_totals": """
        proc sql;
          create table out.card_totals as select merchant, sum(amount) as total
          from stg.card_transactions group by merchant;
        quit;""",
    "tie": """
        data out.loan_card; set stg.loans stg.cards; run;""",
}


@pytest.fixture(scope="module")
def labels(tmp_path_factory) -> pd.DataFrame:
    """Write every snippet to a temporary folder, analyze it and classify it with rules v2 (once per module)."""
    folder = tmp_path_factory.mktemp("sas")
    for name, code in SNIPPETS.items():
        (folder / f"{name}.sas").write_text(code, encoding="utf-8")
    inv, deps, _, _ = analyze(folder, folder)
    return classify(inv.fillna({"procs": ""}), deps, SNIPPETS).set_index("program_id")


@pytest.mark.parametrize(
    ("program", "technical"),
    [
        ("logistic_pd", "Statistical Modeling"),  # v1.1 only knew PROC REG
        ("arima", "Forecasting"),  # v1.1 only knew PROC FORECAST
        ("call_execute", "Orchestration"),  # code generated at run time
        ("macro_only", "Utility"),
        ("proc_report", "Reporting"),  # found from the procedure, not the file name
        ("ods_excel", "Reporting"),  # ODS output wins over the PROC MEANS inside it
        ("hash_lookup", "Transformation"),
        ("card_totals", "Aggregation"),  # SQL GROUP BY
        ("raw_load", "Data Preparation"),
    ],
)
def test_technical(labels, program, technical):
    """Each snippet gets the technical type its rule is meant to produce."""
    assert labels.loc[program, "technical_category"] == technical


@pytest.mark.parametrize(
    ("program", "business", "confidence"),
    [
        ("logistic_pd", "Risk", "HIGH"),  # output pd_model; "default" alone would not decide
        ("macro_only", "Platform", "HIGH"),
        ("raw_load", "Platform", "HIGH"),  # INFILE = ingestion
        ("card_totals", "Payments", "HIGH"),
        ("passthrough", "Payments", "HIGH"),
        ("region_sales", "Other", "LOW"),  # "reg" no longer matches "region" (it did in v1.1)
        ("tie", "Other", "LOW"),  # equal evidence: no guess
    ],
)
def test_business(labels, program, business, confidence):
    """Each snippet gets the expected business area and confidence."""
    assert tuple(labels.loc[program, ["business_category", "business_confidence"]]) == (business, confidence)


def test_complexity_counts_new_patterns(labels):
    """The new v2 complexity signals appear in the reason with their weights."""
    reasons = labels.reason
    assert "has_call_execute 4" in reasons["call_execute"]
    assert "has_hash 4" in reasons["hash_lookup"]
    assert "has_passthrough 3" in reasons["passthrough"]


def test_long_simple_program_stays_simple(tmp_path):
    """400 trivial lines stay Simple: line points are capped."""
    code = "data out.copy; set stg.source; run;\n" + "x = 1;\n" * 400  # 400 trivial lines
    (tmp_path / "long.sas").write_text(code, encoding="utf-8")
    inv, deps, _, _ = analyze(tmp_path, tmp_path)
    result = classify(inv.fillna({"procs": ""}), deps, {"long": code}).iloc[0]
    assert result.complexity == "Simple"  # lines are capped at loc_points_max
    assert result.complexity_score <= RULES["complexity"]["loc_points_max"]


def test_case_programs_get_valid_labels():
    """On the 12 case programs, every label is one of the allowed values."""
    path = ROOT / "outputs" / "rule_analysis_v2.csv"
    if not path.exists():
        pytest.skip("run python/classify_rules_v2.py first")
    out = pd.read_csv(path)
    technical = {r["label"] for r in RULES["technical"]}
    business = {*RULES["business"]["keywords"], "Platform", "Other"}
    assert len(out) == 12
    assert set(out.technical_category) <= technical
    assert set(out.business_category) <= business
    assert set(out.complexity) <= {"Simple", "Moderate", "Complex"}
