"""Checks for the generated dataset. Run from the project root: pytest -q"""

import hashlib
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"


def run_generator() -> None:
    """Run the data generator as a separate process, as a user would."""
    subprocess.run(
        [sys.executable, str(ROOT / "python" / "generate_data.py")],
        check=True,
        cwd=ROOT,
    )


def fingerprint() -> dict[str, str]:
    """SHA-256 of every generated CSV, to compare two runs byte by byte."""
    return {f.name: hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(RAW.glob("*.csv"))}


def setup_module() -> None:
    """Generate the data once before the tests."""
    run_generator()


def test_seven_files_with_expected_sizes():
    """7 tables; the main ones have their planned sizes."""
    sizes = {f.stem: len(pd.read_csv(f)) for f in RAW.glob("*.csv")}
    assert len(sizes) == 7
    assert sizes["customers"] == 500 and sizes["loans"] == 800 and sizes["card_transactions"] == 5000


def test_payments_cover_48_months_with_one_duplicate():
    """Payments span all 48 months and contain exactly one duplicated key (planted)."""
    p = pd.read_csv(RAW / "loan_payments.csv")
    assert p["payment_date"].nunique() == 48

    assert p.duplicated(["loan_id", "payment_date"]).sum() == 1


def test_planted_customer_cases():
    """15 missing incomes, a 30-byte name, an accented name, and 'qc' everywhere."""
    c = pd.read_csv(RAW / "customers.csv")
    assert c["annual_income"].isna().sum() == 15
    assert c["customer_name"].map(lambda s: len(s.encode("utf-8"))).max() == 30
    assert "Émilie Tremblay" in set(c["customer_name"])
    assert (c["province"] == "qc").all()


def test_accented_showcase_names():
    """All four planted accented names are present, unchanged."""
    names = set(pd.read_csv(RAW / "customers.csv")["customer_name"])
    for name in ["Émilie Tremblay", "François Bélanger", "Chloé Gagnon", "Marie-Ève Beauchemin-Laflamme"]:
        assert name in names


def test_apostrophe_city_survives_csv():
    """L'Assomption is read back intact and no city value was split by the apostrophe."""
    c = pd.read_csv(RAW / "customers.csv")
    assert (c["city"] == "L'Assomption").sum() > 0
    assert set(c["city"]) <= {
        "Montréal",
        "Québec",
        "Châteauguay",
        "Trois-Rivières",
        "Lévis",
        "Saint-Jérôme",
        "Gatineau",
        "Sherbrooke",
        "L'Assomption",
        "Rivière-du-Loup",
    }  # no value shifted or split by the apostrophe


def test_card_edge_cases():
    """Exactly 25 refunds and 5 zero amounts (planted)."""
    t = pd.read_csv(RAW / "card_transactions.csv")
    assert (t["amount"] < 0).sum() == 25 and (t["amount"] == 0).sum() == 5


def test_reproducible():
    """Running the generator again gives byte-identical files."""
    before = fingerprint()
    run_generator()
    assert fingerprint() == before


def test_orphan_loan():
    """Exactly one loan points to a customer that does not exist (planted: 999)."""
    customers = pd.read_csv(RAW / "customers.csv")
    loans = pd.read_csv(RAW / "loans.csv")
    assert (~loans["customer_id"].isin(customers["customer_id"])).sum() == 1
