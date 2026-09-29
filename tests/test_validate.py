"""The validator must reject wrong conversions. Run from the project root: pytest -q

Starts from the correct output of program 01 (SAS's own table) and breaks it the ways a careless
conversion would; each broken version must fail, with the right check naming the problem.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
from migrate_data import read_sas
from validate import validate_program


def write(df, folder: Path) -> Path:
    """Write a table as the converted code would: a folder holding a Parquet file."""
    (folder / "customers_clean").mkdir(parents=True)
    df.to_parquet(folder / "customers_clean" / "part-0.parquet", index=False)
    return folder


@pytest.fixture
def correct():
    """SAS's own output of program 01: a perfect conversion would produce exactly this."""
    return read_sas(ROOT / "sas" / "outputs" / "customers_clean.sas7bdat")[0]


def failed(pid, out_dir) -> set[str]:
    """Names of the checks that failed."""
    return {c["check"] for c in validate_program(pid, out_dir) if not c["passed"]}


def test_correct_output_passes(correct, tmp_path):
    """Control: SAS's own table passes every check, including the planted ones."""
    assert failed("01_customer_clean", write(correct, tmp_path)) == set()


def test_missing_income_trap_is_caught(correct, tmp_path):
    """The classic trap: missing incomes put in HIGH (Python's comparison with null) instead of LOW."""
    wrong = correct.copy()
    wrong.loc[wrong.annual_income.isna(), "income_band"] = "HIGH"
    fails = failed("01_customer_clean", write(wrong, tmp_path))
    assert "planted_missing_income_is_low" in fails
    assert "exact_text_values" in fails and "row_hash_sha256" in fails


def test_skipped_cleaning_rule_is_caught(correct, tmp_path):
    """A skipped rule: province left as 'qc'."""
    wrong = correct.assign(province="qc")
    assert "planted_province_upper_case" in failed("01_customer_clean", write(wrong, tmp_path))


def test_lost_row_is_caught(correct, tmp_path):
    """A lost row changes the row count."""
    assert "row_count" in failed("01_customer_clean", write(correct.iloc[1:], tmp_path))
