"""forecast.py - reproduce SAS PROC FORECAST METHOD=EXPO TREND=2 (Brown double exponential smoothing).

Only this configuration is reproduced: explicit WEIGHT= and NSTART=, no missing values inside the series.
Compare the result with SAS OUTEST (S1, S2, CONSTANT, LINEAR) and OUT=/OUTFULL (_TYPE_='FORECAST').

Run:  python python/forecast.py --parity    compare with the SAS results in sas/outputs/ (exit code 1 if FAIL)
      python python/forecast.py             quick demo on the raw CSV (no SAS needed)
Out:  outputs/forecast_parity.csv (summary), outputs/forecast_parity_detail.csv (month by month)

If the numbers drift, check period by period (see guide):
  1) time origin of the start-up regression (t = 1..NSTART here; try 0..NSTART-1 if S1/S2 differ),
  2) whether SAS's in-sample FORECAST at t uses states up to t-1 (implemented here: yes, one-step-ahead).
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def brown_expo(y, weight=0.2, nstart=12, lead=12, t0=1):
    """Return (fitted one-step-ahead forecasts, future forecasts, final states dict)."""
    y = np.asarray(y, dtype=float)
    w, b = weight, 1.0 - weight
    # 1) start values: OLS time-trend regression on the first NSTART observations
    t = np.arange(t0, t0 + nstart)
    slope, intercept = np.polyfit(t, y[:nstart], 1)
    a0 = intercept + slope * (t0 - 1)  # level just before the first observation
    s1 = a0 - (b / w) * slope
    s2 = a0 - 2 * (b / w) * slope
    fitted = []
    for yt in y:
        level, trend = 2 * s1 - s2, (w / b) * (s1 - s2)
        fitted.append(level + trend)  # forecast for t made at t-1
        s1 = w * yt + b * s1  # update smoothed values with y[t]
        s2 = w * s1 + b * s2
    level, trend = 2 * s1 - s2, (w / b) * (s1 - s2)
    future = [level + trend * k for k in range(1, lead + 1)]
    return np.array(fitted), np.array(future), {"S1": s1, "S2": s2, "CONSTANT": level, "LINEAR": trend}


def compare(py_values, sas_values, tol=1e-6):
    """Absolute differences between two series: count, max, RMSE, how many exceed tol, PASS/FAIL."""
    d = np.abs(np.asarray(py_values) - np.asarray(sas_values))
    return {
        "n": len(d),
        "max_abs_diff": float(d.max()),
        "rmse": float(np.sqrt((d**2).mean())),
        "n_above_tol": int((d > tol).sum()),
        "status": "PASS" if (d <= tol).all() else "FAIL",
    }


def demo():
    """Forecast the monthly repayments computed from the raw CSV (a quick check without SAS)."""
    p = pd.read_csv(ROOT / "data" / "raw" / "loan_payments.csv", parse_dates=["payment_date"])
    series = p.groupby(p.payment_date.dt.to_period("M")).payment_amount.sum()
    _, future, states = brown_expo(series.values)
    print("final states:", {k: round(float(v), 4) for k, v in states.items()})
    print("next 3 months:", np.round(future[:3], 2))


def parity(weight=0.2, nstart=12, lead=12, tol=1e-6):
    """Compare this implementation with SAS (program 10's outputs); return True when everything PASSes.

    Input is SAS's own monthly totals (out.monthly_repay), so only the forecasting method is tested;
    the monthly aggregation is tested with the converted program 10 (see validate.py).
    """
    sys.path.insert(0, str(Path(__file__).parent))
    from migrate_data import read_sas

    sas = ROOT / "sas" / "outputs"
    hist = read_sas(sas / "monthly_repay.sas7bdat")[0].sort_values("month")
    fc = read_sas(sas / "fc.sas7bdat")[0]
    est = read_sas(sas / "fc_est.sas7bdat")[0]
    fitted, future, states = brown_expo(hist.total_payment.values, weight, nstart, lead)
    sas_fc = fc[fc["_type_"].str.upper() == "FORECAST"].sort_values("month")
    detail = pd.DataFrame(
        {
            "month": pd.to_datetime(sas_fc.month).values,  # SAS's own months: a shift would be visible
            "sas_forecast": sas_fc.total_payment.values,
            "python_forecast": np.concatenate([fitted, future]),
            "is_future": [0] * len(fitted) + [1] * len(future),
        }
    )
    detail["abs_diff"] = (detail.sas_forecast - detail.python_forecast).abs()
    expected_months = list(pd.to_datetime(hist.month)) + list(
        pd.date_range(pd.to_datetime(hist.month).max(), periods=lead + 1, freq="MS")[1:]
    )
    rows = [
        {"item": "forecast_values", **compare(detail.python_forecast, detail.sas_forecast, tol)},
        {
            "item": "forecast_months",
            "n": len(detail),
            "max_abs_diff": 0.0,
            "rmse": 0.0,
            "n_above_tol": int(sum(a != b for a, b in zip(detail.month, expected_months))),
            "status": "PASS" if list(detail.month) == expected_months else "FAIL",
        },
    ]
    est_vals = dict(zip(est["_type_"].str.upper(), est.total_payment))
    for k in ("S1", "S2", "CONSTANT", "LINEAR"):
        rows.append({"item": f"final_state_{k}", **compare([states[k]], [est_vals[k]], tol)})
    summary = pd.DataFrame(rows)
    detail.to_csv(ROOT / "outputs" / "forecast_parity_detail.csv", index=False)
    summary.to_csv(ROOT / "outputs" / "forecast_parity.csv", index=False)
    print(summary.to_string(index=False))
    return bool((summary.status == "PASS").all())


def main():
    """--parity: compare with SAS (exit code 1 on FAIL); no option: demo on the raw CSV."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--parity", action="store_true", help="compare with the SAS results")
    args = parser.parse_args()
    if args.parity:
        sys.exit(0 if parity() else 1)
    demo()


if __name__ == "__main__":
    main()
