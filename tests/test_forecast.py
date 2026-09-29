"""Checks for the hand-built PROC FORECAST. Run from the project root: pytest -q"""

import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
from forecast import brown_expo


def test_straight_line_is_forecast_exactly():
    """A correct double exponential smoothing reproduces a perfect trend line and continues it."""
    y = np.arange(1, 40) * 3.0 + 5
    fitted, future, _ = brown_expo(y)
    assert np.abs(fitted - y).max() < 1e-9
    assert np.allclose(future[:3], [125, 128, 131])


def test_parity_with_sas():
    """Every forecast value, the months and the 4 final states match SAS within 1e-6 (exit code 0)."""
    subprocess.run([sys.executable, str(ROOT / "python" / "forecast.py"), "--parity"], check=True, cwd=ROOT)
    summary = pd.read_csv(ROOT / "outputs" / "forecast_parity.csv")
    assert len(summary) == 6 and (summary.status == "PASS").all()
    assert summary.loc[summary.item == "forecast_values", "n"].item() == 60  # 48 fitted + 12 future
