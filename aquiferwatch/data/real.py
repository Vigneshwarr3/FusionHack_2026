"""Real-data feature builders for notebooks 01 and 02.

Parallel to `synthetic.py` / `synthetic_depletion.py`. The notebooks import from
one or the other depending on which data path they want. Column contracts match
so the rest of each notebook stays identical.

What's real today
-----------------
- **NB01 extraction imputation**: cross-sectional (single reference year) using
  `baseline.parquet`. The 248 `modeled_high` counties become labeled training
  rows; the 358 `modeled_low` counties are targets for imputation. Note this
  is *not* a time panel \u2014 waiting on KS Open Records Request for that.

- **NB02 depletion projection**: per-county annual thickness panel built from
  TWDB water-level history (TX, `sat_thickness_m` precomputed) + KGS WIZARD
  (KS, thickness = bedrock_depth \u2212 depth_to_water using the county bedrock
  snapshot). Multi-year, covers ~120 counties depending on well coverage.

Missing vs. synthetic
---------------------
- `precip_mm`: PRISM ingest deferred. NB01 drops this feature.
- `well_density`: we have `n_wells` per county in baseline but not per-km\u00b2.
  NB01 uses `n_wells` as a proxy.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from aquiferwatch.data.loaders import (
    load_baseline,
    load_twdb_levels,
    load_twdb_wells,
    load_wizard_levels,
    load_wizard_sites,
    load_parquet,
)

# ---------------------------------------------------------------------------
# NB01 \u2014 extraction imputation (cross-sectional, from baseline)
# ---------------------------------------------------------------------------

CROPS = ["corn", "soybeans", "sorghum", "wheat", "cotton", "alfalfa"]

# Subset of synthetic.FEATURE_COLS that real data supports today.
# Dropped: precip_mm (PRISM pending). well_density \u2192 swapped to n_wells.
REAL_EXTRACTION_FEATURE_COLS = [
    "saturated_thickness_m",
    "n_wells",
    "grid_kwh_per_af",
    *[f"irrigated_acres_{c}" for c in CROPS],
]
REAL_EXTRACTION_TARGET_COL = "pumpage_af"


def build_extraction_dataset(
    quality_filter: str | None = "modeled_high",
    reference_year: int = 2020,
) -> pd.DataFrame:
    """Cross-sectional extraction dataset from `baseline.parquet`.

    Parameters
    ----------
    quality_filter : str or None
        If set, keep only rows with that `data_quality` tag. Default
        `"modeled_high"` gives 248 counties with real thickness + decline
        measurements \u2014 the proper training set. Pass `None` to get all 606
        rows (for imputation inference).
    reference_year : int
        Year stamped on every row. Baseline is a snapshot, so all rows get the
        same year. Kept as a column for API symmetry with synthetic.

    Returns a DataFrame with columns matching `synthetic.generate_tx_extraction_dataset`
    minus `precip_mm` (not available) and with `n_wells` in place of `well_density`.
    """
    df = load_baseline().copy()
    if quality_filter:
        df = df[df["data_quality"] == quality_filter].copy()

    out = pd.DataFrame({
        "fips": df["fips"].astype(str),
        "year": reference_year,
        "state": df["state"],
        "saturated_thickness_m": df["saturated_thickness_m"],
        "n_wells": df["n_wells"].fillna(0),
        "grid_kwh_per_af": df["kwh_per_af_pumped"],
        REAL_EXTRACTION_TARGET_COL: df["pumping_af_yr"],
    })
    for crop in CROPS:
        out[f"irrigated_acres_{crop}"] = df[f"acres_{crop}"].fillna(0)

    out = out.dropna(subset=[REAL_EXTRACTION_TARGET_COL, "saturated_thickness_m"])
    return out.reset_index(drop=True)


def spatial_train_test_split(
    df: pd.DataFrame, test_frac: float = 0.2, seed: int = 42
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Whole-county hold-out split. Mirrors `synthetic.spatial_train_test_split`."""
    rng = np.random.default_rng(seed)
    counties = df["fips"].unique()
    rng.shuffle(counties)
    n_test = max(1, int(len(counties) * test_frac))
    test_counties = set(counties[:n_test])
    test = df[df["fips"].isin(test_counties)].copy()
    train = df[~df["fips"].isin(test_counties)].copy()
    return train, test


# ---------------------------------------------------------------------------
# NB02 \u2014 depletion projection (per-county annual thickness panel)
# ---------------------------------------------------------------------------

def _twdb_annual_thickness() -> pd.DataFrame:
    """Per-county-per-year median saturated thickness from TWDB TX wells."""
    levels = load_twdb_levels()
    wells = load_twdb_wells()[["StateWellNumber", "fips"]].dropna()

    merged = levels.merge(wells, on="StateWellNumber", how="inner")
    merged = merged.dropna(subset=["sat_thickness_m", "MeasurementDate", "fips"])
    merged["year"] = pd.to_datetime(merged["MeasurementDate"]).dt.year
    merged["fips"] = merged["fips"].astype(str).str.zfill(5)

    panel = (
        merged.groupby(["fips", "year"], as_index=False)["sat_thickness_m"]
        .median()
        .rename(columns={"sat_thickness_m": "saturated_thickness_m"})
    )
    panel["state"] = "TX"
    panel["source"] = "twdb"
    return panel


def _wizard_annual_thickness() -> pd.DataFrame:
    """Per-county-per-year median saturated thickness from KGS WIZARD wells.

    sat_thickness_m = (bedrock_depth \u2212 depth_to_water), ft \u2192 m.
    Uses the county-level bedrock snapshot (`kgs_county_bedrock.parquet`) since
    per-well bedrock depth isn't reliably populated in WIZARD sites.
    """
    levels = load_wizard_levels()
    sites = load_wizard_sites()[["USGS_ID", "fips"]].dropna()
    bedrock = load_parquet("kgs_county_bedrock.parquet")[["fips", "median_bed_depth_ft"]]

    merged = levels.merge(sites, on="USGS_ID", how="inner")
    merged = merged.dropna(subset=["DEPTH_TO_WATER", "MEASUREMENT_DATE_AND_TIME", "fips"])
    merged["year"] = pd.to_datetime(merged["MEASUREMENT_DATE_AND_TIME"]).dt.year
    merged["fips"] = merged["fips"].astype(str).str.zfill(5)

    merged = merged.merge(bedrock, on="fips", how="inner")
    merged["sat_thickness_ft"] = merged["median_bed_depth_ft"] - merged["DEPTH_TO_WATER"]
    merged["sat_thickness_m"] = merged["sat_thickness_ft"] * 0.3048

    merged = merged[(merged["sat_thickness_m"] > 0) & (merged["sat_thickness_m"] < 300)]

    panel = (
        merged.groupby(["fips", "year"], as_index=False)["sat_thickness_m"]
        .median()
        .rename(columns={"sat_thickness_m": "saturated_thickness_m"})
    )
    panel["state"] = "KS"
    panel["source"] = "wizard"
    return panel


def build_depletion_history(
    min_year: int = 1990,
    max_year: int | None = None,
    min_points_per_county: int = 6,
    sources: tuple[str, ...] = ("twdb", "wizard"),
) -> pd.DataFrame:
    """Real per-county annual thickness panel, ready for `build_lagged_features`.

    Joins TWDB TX + KGS WIZARD KS water-level histories, adds snapshot
    covariates from `baseline.parquet` (recharge, pumping, decline rate).

    Parameters
    ----------
    min_year, max_year : int
        Trim the panel to a realistic modeling window. WIZARD records start
        ~1935 but signal is noisy pre-1990.
    min_points_per_county : int
        Drop counties with too few years to support lag features.
    sources : tuple[str, ...]
        Which source builders to include. Default both.

    Returns
    -------
    DataFrame with columns:
        fips, year, state, saturated_thickness_m,
        recharge_mm_yr, pumping_af_yr, decline_rate_m_yr, source
    """
    frames = []
    if "twdb" in sources:
        frames.append(_twdb_annual_thickness())
    if "wizard" in sources:
        frames.append(_wizard_annual_thickness())
    if not frames:
        raise ValueError("no sources selected")

    panel = pd.concat(frames, ignore_index=True)
    panel = panel[panel["year"] >= min_year]
    if max_year:
        panel = panel[panel["year"] <= max_year]

    counts = panel.groupby("fips")["year"].transform("count")
    panel = panel[counts >= min_points_per_county]

    baseline = load_baseline()[["fips", "recharge_mm_yr", "pumping_af_yr", "annual_decline_m"]]
    baseline = baseline.rename(columns={"annual_decline_m": "decline_rate_m_yr"})
    baseline["fips"] = baseline["fips"].astype(str).str.zfill(5)

    panel = panel.merge(baseline, on="fips", how="left")
    panel = panel.sort_values(["fips", "year"]).reset_index(drop=True)
    return panel


def build_lagged_features(
    history: pd.DataFrame, lags: tuple[int, ...] = (1, 2, 3, 5)
) -> pd.DataFrame:
    """Same contract as `synthetic_depletion.build_lagged_features`.

    Duplicated here so notebooks can import from one module without caring
    whether the upstream `history` is synthetic or real.
    """
    df = history.sort_values(["fips", "year"]).copy()
    for lag in lags:
        df[f"thickness_lag{lag}"] = df.groupby("fips")["saturated_thickness_m"].shift(lag)
    df["trend_5y"] = (
        df["saturated_thickness_m"] - df.groupby("fips")["saturated_thickness_m"].shift(5)
    )
    df["pumping_lag1"] = df["pumping_af_yr"]
    df["y_next_thickness"] = df.groupby("fips")["saturated_thickness_m"].shift(-1)
    return df.dropna()


# Re-exported so notebook imports are a single line swap.
REAL_DEPLETION_FEATURE_COLS = [
    "thickness_lag1", "thickness_lag2", "thickness_lag3", "thickness_lag5",
    "trend_5y", "pumping_lag1", "recharge_mm_yr",
]
REAL_DEPLETION_TARGET_COL = "y_next_thickness"
