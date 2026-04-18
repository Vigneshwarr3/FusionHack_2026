"""Baseline loader for the scenario engine.

Real baseline is assembled from the pipeline and written to
`data/processed/baseline.parquet`. Until that's in place, we fall back to a
small synthetic baseline so the API + scenario engine can be exercised
end-to-end during development (and the frontend can integrate early).

Swap-the-loader: production code should call `load_baseline()` and get the
real parquet when present; no other module needs to know about the fallback.
"""

from __future__ import annotations

import pandas as pd

from aquiferwatch.config import DATA_DIR

BASELINE_PATH = DATA_DIR / "processed" / "baseline.parquet"


def load_baseline() -> pd.DataFrame:
    """Prefer real parquet; fall back to synthetic 3-county sample."""
    if BASELINE_PATH.exists():
        return pd.read_parquet(BASELINE_PATH)
    return _make_toy_baseline()


def _make_toy_baseline() -> pd.DataFrame:
    """Three-county synthetic baseline — same shape as test fixture.

    Finney KS (healthy), Dallam TX (past threshold), Hooker NE (stable).
    """
    from aquiferwatch.analytics.scenarios import (
        CROPS,
        METHOD_EFFICIENCY,
        REVENUE_PER_ACRE,
        WATER_PER_ACRE,
    )

    rows = [
        dict(
            fips="20055", state="KS",
            saturated_thickness_m=22.0, annual_decline_m=-0.38, recharge_mm_yr=15.0,
            irrigated_acres_total=168_000,
            acres_corn=100_000, acres_soybeans=10_000, acres_sorghum=20_000,
            acres_wheat=25_000, acres_cotton=3_000, acres_alfalfa=10_000,
            irr_center_pivot=0.85, irr_flood=0.05, irr_drip=0.05, irr_dryland=0.05,
            kwh_per_af_pumped=250.0, grid_intensity_kg_per_kwh=0.45,
        ),
        dict(
            fips="48111", state="TX",
            saturated_thickness_m=7.5, annual_decline_m=-0.55, recharge_mm_yr=8.0,
            irrigated_acres_total=240_000,
            acres_corn=80_000, acres_soybeans=5_000, acres_sorghum=30_000,
            acres_wheat=20_000, acres_cotton=100_000, acres_alfalfa=5_000,
            irr_center_pivot=0.70, irr_flood=0.20, irr_drip=0.05, irr_dryland=0.05,
            kwh_per_af_pumped=320.0, grid_intensity_kg_per_kwh=0.52,
        ),
        dict(
            fips="31091", state="NE",
            saturated_thickness_m=180.0, annual_decline_m=-0.05, recharge_mm_yr=95.0,
            irrigated_acres_total=85_000,
            acres_corn=40_000, acres_soybeans=25_000, acres_sorghum=5_000,
            acres_wheat=10_000, acres_cotton=0, acres_alfalfa=5_000,
            irr_center_pivot=0.95, irr_flood=0.00, irr_drip=0.05, irr_dryland=0.00,
            kwh_per_af_pumped=180.0, grid_intensity_kg_per_kwh=0.62,
        ),
    ]
    df = pd.DataFrame(rows)
    df["ag_value_usd"] = sum(df[f"acres_{c}"] * REVENUE_PER_ACRE[c] for c in CROPS)
    base_water = sum(df[f"acres_{c}"] * WATER_PER_ACRE[c] for c in CROPS)
    eff = (
        df["irr_center_pivot"] * METHOD_EFFICIENCY["center_pivot"]
        + df["irr_flood"] * METHOD_EFFICIENCY["flood"]
        + df["irr_drip"] * METHOD_EFFICIENCY["drip"]
        + df["irr_dryland"] * METHOD_EFFICIENCY["dryland"]
    )
    df["pumping_af_yr"] = base_water * eff
    df["employment_fte"] = df["ag_value_usd"] * (0.021 / 1000.0)
    return df
