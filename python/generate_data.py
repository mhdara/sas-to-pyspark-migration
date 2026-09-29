"""Generate the synthetic Québec retail-bank dataset for the SAS migration case.

Run from the project root:
    python python/generate_data.py

Writes UTF-8 CSV files to data/raw/. Same SEED -> byte-identical files on every run.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------- configuration
SEED = 42
PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"

MONTHS = pd.date_range("2022-01-01", "2025-12-01", freq="MS")  # 48 month starts
N_CUSTOMERS = 500
N_LOANS = 800
N_CARD_TRANSACTIONS = 5_000

FIRST_NAMES = [
    "Émilie",
    "François",
    "Chloé",
    "Jérôme",
    "Hélène",
    "Zoé",
    "Gaëlle",
    "Mathieu",
    "Sophie",
    "Olivier",
    "Noémie",
    "André",
    "Léa",
    "Benoît",
    "Céline",
    "Marc",
    "Josée",
    "Rémi",
    "Anaïs",
    "Luc",
]
LAST_NAMES = [
    "Tremblay",
    "Bélanger",
    "Gagnon",
    "Côté",
    "Bouchard",
    "Gauthier",
    "Morin",
    "Lévesque",
    "Pelletier",
    "Bergeron",
    "Leblanc",
    "Paquette",
    "Desjardins",
    "Girard",
    "Ménard",
]
CITIES = [
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
]

SEGMENTS = [  # code, French label, risk band, min income, max income
    ("RETAIL", "Particulier Québec", "MEDIUM", 0, 60_000),
    ("PREMIUM", "Clientèle privilégiée", "LOW", 60_000, 150_000),
    ("PRIVATE", "Gestion privée", "LOW", 150_000, 10_000_000),
    ("SME", "Petite entreprise", "HIGH", 0, 10_000_000),
    ("STUDENT", "Étudiant", "HIGH", 0, 30_000),
]
SEGMENT_WEIGHTS = [0.55, 0.20, 0.05, 0.10, 0.10]

PLANTED_NAMES = {
    7: "Marie-Ève Beauchemin-Laflamme",  # 30 bytes in UTF-8
    42: "Émilie Tremblay",
    43: "François Bélanger",
    44: "Chloé Gagnon",
}
N_MISSING_INCOMES = 15
N_REFUNDS = 25
N_ZERO_AMOUNTS = 5


# ---------------------------------------------------------------- tables
def make_control_tables() -> dict[str, pd.DataFrame]:
    """Configuration tables: written by hand, never random."""
    segments = pd.DataFrame(
        SEGMENTS,
        columns=[
            "segment_code",
            "segment_name",
            "risk_band",
            "min_income",
            "max_income",
        ],
    )
    last_12 = MONTHS[-12:]
    periods = pd.DataFrame(
        {
            "period_id": last_12.strftime("%Y%m"),
            "period_start": last_12,
            "period_label": last_12.strftime("%Y-%m"),
            "active_flag": [0] * 8 + [1] * 4,
        }
    )
    parameters = pd.DataFrame(
        {  # values stored as TEXT on purpose, like real config tables
            "param_name": ["DPD_THRESHOLD", "FRAUD_MIN_AMOUNT", "HIGH_PRINCIPAL"],
            "param_value": ["30", "500", "5000000"],
        }
    )
    return {
        "ctrl_client_segments": segments,
        "ctrl_reporting_periods": periods,
        "ctrl_macro_parameters": parameters,
    }


def make_customers(rng: np.random.Generator) -> pd.DataFrame:
    """500 customers with Québec names and cities; planted: accented and 30-byte names, 15 missing incomes, 'qc'."""
    names = [f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}" for _ in range(N_CUSTOMERS)]
    for position, name in PLANTED_NAMES.items():
        names[position] = name
    income = rng.lognormal(mean=10.9, sigma=0.5, size=N_CUSTOMERS).round(0)  # median ~ 54,000
    income[rng.choice(N_CUSTOMERS, size=N_MISSING_INCOMES, replace=False)] = np.nan
    return pd.DataFrame(
        {
            "customer_id": np.arange(1, N_CUSTOMERS + 1),
            "customer_name": names,
            "city": rng.choice(CITIES, N_CUSTOMERS),
            "province": "qc",  # lower case on purpose: the cleaning program upper-cases it
            "segment_code": rng.choice([s[0] for s in SEGMENTS], N_CUSTOMERS, p=SEGMENT_WEIGHTS),
            "annual_income": income,
            "customer_since": pd.Timestamp("2010-01-01")
            + pd.to_timedelta(rng.integers(0, 4000, N_CUSTOMERS), unit="D"),
        }
    )


def make_loans(rng: np.random.Generator) -> pd.DataFrame:
    """800 loans with annuity monthly payments; start dates sorted (rising volume); planted: customer 999."""
    loan_type = rng.choice(["MORTGAGE", "AUTO", "PERSONAL", "CREDIT_LINE"], N_LOANS, p=[0.3, 0.3, 0.3, 0.1])
    principal = np.select(
        [loan_type == "MORTGAGE", loan_type == "AUTO"],
        [
            rng.integers(150_000, 600_000, N_LOANS),
            rng.integers(15_000, 60_000, N_LOANS),
        ],
        default=rng.integers(2_000, 30_000, N_LOANS),
    ).astype(float)
    rate = rng.choice([0.0399, 0.0549, 0.0699, 0.0899, 0.1199], N_LOANS)
    term = np.where(loan_type == "MORTGAGE", 300, rng.choice([36, 48, 60], N_LOANS))
    r = rate / 12  # monthly rate
    payment = (principal * r / (1 - (1 + r) ** (-term))).round(2)  # annuity formula
    start = np.sort(rng.integers(0, 44, N_LOANS))  # sorted -> volume grows = trend
    loans = pd.DataFrame(
        {
            "loan_id": np.arange(10_001, 10_001 + N_LOANS),
            "customer_id": rng.integers(1, N_CUSTOMERS + 1, N_LOANS),
            "loan_type": loan_type,
            "origination_date": MONTHS[start],
            "principal": principal,
            "interest_rate": rate,
            "term_months": term,
            "status": rng.choice(["ACTIVE", "CLOSED", "DEFAULT"], N_LOANS, p=[0.85, 0.10, 0.05]),
            "monthly_payment": payment,
        },
    )
    loans.loc[N_LOANS - 1, "customer_id"] = 999

    return loans


def make_payments(rng: np.random.Generator, loans: pd.DataFrame) -> pd.DataFrame:
    """One payment row per loan and month from its start date; about 10% 45 days late (unpaid); planted: one duplicate row."""
    rows = []
    for loan in loans.itertuples(index=False):
        balance, r = loan.principal, loan.interest_rate / 12
        for month in MONTHS[MONTHS >= loan.origination_date]:  # simplification: ignores maturity
            interest = round(balance * r, 2)
            dpd = int(rng.choice([0, 0, 0, 0, 0, 0, 0, 0, 15, 45]))  # 80% on time, 10% 15d, 10% 45d
            paid = 0.0 if dpd >= 45 else loan.monthly_payment  # 45 days late = missed payment
            principal_paid = round(max(paid - interest, 0), 2)
            balance = max(balance - principal_paid, 0)
            rows.append(
                (
                    loan.loan_id,
                    month,
                    paid,
                    principal_paid,
                    interest if paid else 0.0,
                    dpd,
                )
            )
    payments = pd.DataFrame(
        rows,
        columns=[
            "loan_id",
            "payment_date",
            "payment_amount",
            "principal_paid",
            "interest_paid",
            "days_past_due",
        ],
    )
    return pd.concat([payments, payments.iloc[[100]]], ignore_index=True)  # planted duplicate key


def make_card_transactions(rng: np.random.Generator) -> pd.DataFrame:
    """5,000 card transactions in 2025 with skewed amounts; planted: 25 refunds and 5 zero amounts."""
    amount = rng.gamma(shape=2.0, scale=60.0, size=N_CARD_TRANSACTIONS).round(2)  # mean 120, skewed
    # one draw for both planted cases, so a zero can never overwrite a refund
    special = rng.choice(N_CARD_TRANSACTIONS, N_REFUNDS + N_ZERO_AMOUNTS, replace=False)
    amount[special[:N_REFUNDS]] *= -1  # refunds
    amount[special[N_REFUNDS:]] = 0.0  # zero amounts
    return pd.DataFrame(
        {
            "transaction_id": np.arange(1, N_CARD_TRANSACTIONS + 1),
            "customer_id": rng.integers(1, N_CUSTOMERS + 1, N_CARD_TRANSACTIONS),
            "transaction_date": pd.Timestamp("2025-01-01")
            + pd.to_timedelta(rng.integers(0, 365, N_CARD_TRANSACTIONS), unit="D"),
            "merchant_category": rng.choice(
                ["GROCERY", "TRAVEL", "RESTAURANT", "ONLINE", "FUEL", "ELECTRONICS"],
                N_CARD_TRANSACTIONS,
            ),
            "amount": amount,
            "fraud_flag": (rng.random(N_CARD_TRANSACTIONS) < 0.01).astype(int),
        }
    )


# ---------------------------------------------------------------- output
def write_csv(df: pd.DataFrame, name: str) -> None:
    """Write one table as UTF-8 CSV to data/raw/ (dates as YYYY-MM-DD, empty cell = missing)."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(
        OUTPUT_DIR / f"{name}.csv",
        index=False,
        encoding="utf-8",
        date_format="%Y-%m-%d",
    )
    print(f"{name:24s} {len(df):>7,d} rows")


def main() -> None:
    """Generate every table from one random generator (seed 42) and write them; same seed -> identical files."""
    rng = np.random.default_rng(SEED)  # ONE generator, created once, passed to every function
    tables = make_control_tables()
    tables["customers"] = make_customers(rng)
    tables["loans"] = make_loans(rng)
    tables["loan_payments"] = make_payments(rng, tables["loans"])
    tables["card_transactions"] = make_card_transactions(rng)
    for name, df in tables.items():
        write_csv(df, name)
    longest = tables["customers"]["customer_name"].map(lambda s: len(s.encode("utf-8"))).max()
    print(f"longest customer_name = {longest} bytes in UTF-8 (SAS length must be >= this)")


if __name__ == "__main__":
    main()
