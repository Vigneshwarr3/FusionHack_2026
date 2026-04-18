"""Synthetic saturated-thickness time series for depletion forecasting.

Parallel purpose to `synthetic.py`: lets us iterate on the forecasting pipeline
before real USGS / KGS data lands. Same swap-the-loader discipline applies.

Generative model:
    thickness(t) = thickness(t-1) + recharge_mm/1000 - pumping_af*0.3 + noise

Counties have heterogeneous decline rates (fast in SW Kansas / TX panhandle,
slow in Nebraska Sandhills), producing a realistic spread.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def generate_thickness_history(
    n_counties: int = 120,
    n_years: int = 35,
    seed: int = 17,
) -> pd.DataFrame:
    """Per-county, per-year saturated thickness in meters.

    Columns: fips, year, state, saturated_thickness_m, recharge_mm_yr,
             pumping_af_yr, neighbor_mean_thickness_m, decline_rate_m_yr.
    """
    rng = np.random.default_rng(seed)
    states = ["NE", "KS", "CO", "TX", "OK", "NM", "SD", "WY"]
    base = rng.uniform(15, 90, n_counties)
    recharge = rng.uniform(10, 90, n_counties)  # mm/yr
    base_pumping = rng.uniform(20_000, 250_000, n_counties)  # ac-ft/yr
    decline_bias = rng.uniform(0.05, 1.2, n_counties)  # m/yr, dominates signal

    rows = []
    for i in range(n_counties):
        fips = f"{rng.integers(1, 57):02d}{rng.integers(1, 999):03d}"
        state = rng.choice(states)
        thickness = base[i]
        for yr_idx in range(n_years):
            year = 1990 + yr_idx
            pumping = base_pumping[i] * (1 + rng.normal(0, 0.08))
            delta = recharge[i] / 1000 - decline_bias[i] - rng.normal(0, 0.08)
            thickness = max(1.0, thickness + delta)
            rows.append({
                "fips": fips,
                "year": year,
                "state": state,
                "saturated_thickness_m": thickness,
                "recharge_mm_yr": recharge[i],
                "pumping_af_yr": pumping,
                "decline_rate_m_yr": decline_bias[i],
            })
    return pd.DataFrame(rows)


def build_lagged_features(
    history: pd.DataFrame, lags: tuple[int, ...] = (1, 2, 3, 5)
) -> pd.DataFrame:
    """Per-county lag features of saturated thickness + rolling trend.

    The target `y` is next-year thickness; features are everything known at time t.
    """
    df = history.sort_values(["fips", "year"]).copy()
    for lag in lags:
        df[f"thickness_lag{lag}"] = df.groupby("fips")["saturated_thickness_m"].shift(lag)
    df["trend_5y"] = (
        df["saturated_thickness_m"] - df.groupby("fips")["saturated_thickness_m"].shift(5)
    )
    df["pumping_lag1"] = df.groupby("fips")["pumping_af_yr"].shift(1)
    df["y_next_thickness"] = df.groupby("fips")["saturated_thickness_m"].shift(-1)
    return df.dropna()


FEATURE_COLS = [
    "thickness_lag1", "thickness_lag2", "thickness_lag3", "thickness_lag5",
    "trend_5y", "pumping_lag1", "recharge_mm_yr",
]
TARGET_COL = "y_next_thickness"
