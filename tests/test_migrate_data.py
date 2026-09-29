"""Checks for the data migration SAS -> Parquet. Run from the project root: pytest -q"""

import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def results() -> pd.DataFrame:
    """Run the migration once; it must end without an unexpected result (exit code 0)."""
    subprocess.run([sys.executable, str(ROOT / "python" / "migrate_data.py")], check=True, cwd=ROOT)
    return pd.read_csv(ROOT / "outputs" / "reconciliation_results.csv")


def test_every_migration_check_passes(results):
    """All checks that compare SAS with Parquet (and the CSV source with SAS) pass."""
    migration = results[results.expected_to_pass == 1]
    assert len(migration) > 150 and migration.passed.all()


def test_all_seven_tables_are_fingerprinted(results):
    """Every table has a row-by-row SHA-256 comparison, and it matches."""
    hashes = results[results.check == "row_hash_sha256"]
    assert len(hashes) == 7 and hashes.passed.all()


def test_wrong_encoding_is_detected(results):
    """Negative test: reading customers as latin1 must be caught as corruption."""
    assert results.loc[results.check == "negative_test_wrong_encoding", "actual_value"].item() == "detected"


def test_truncation_evidence_fails_as_designed(results):
    """The v1 ($12) load is evidence: every truncation check must fail, including broken characters."""
    evidence = results[results.check.str.startswith("truncation_demo_")]
    assert set(evidence.check) == {
        "truncation_demo_exact_text_values",
        "truncation_demo_max_length_chars",
        "truncation_demo_invalid_utf8",
    }
    assert (evidence.passed == 0).all()


def test_planted_names_identical_everywhere():
    """The four accented showcase names are the same in the CSV, in SAS and in Parquet."""
    names = pd.read_csv(ROOT / "outputs" / "showcase_names.csv")
    assert len(names) == 4
    assert (names.source_csv_value == names.sas_value).all()
    assert (names.sas_value == names.parquet_value).all()
