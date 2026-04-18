"""Synthetic Texas-extraction dataset for notebook experimentation.

Purpose: let Vignesh start iterating on XGBoost / LightGBM / CatBoost / stacking
*today*, without waiting for the USGS + TWDB ingest to land. Replace the data
loader in the notebook once real data arrives — nothing else should change.

Generative model (documented so Vignesh knows what signal to expect):

    pumpage_af = crop_water + depletion_penalty + precip_adjustment + noise

where
    crop_water           = Σ_crop (irrigated_acres_crop × water_per_acre_crop)
    water_per_acre       = {corn:1.4, cotton:1.1, sorghum:0.8, wheat:0.7, alfalfa:1.8}
    depletion_penalty    = +5% pumpage per meter of thickness lost below 30m
                           (deeper lifts make farmers pump harder per acre)
    precip_adjustment    = -pumpage linearly with annual precip mm
                           (wetter years reduce irrigation demand)
    noise                = Gaussian, σ = 8% of mean

This rewards models that find interactions (depletion × crop_mix)
and penalizes models that just learn "acreage → water". Matches the
structure we expect in real TX data.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

CROPS = ["corn", "soybeans", "sorghum", "wheat", "cotton", "alfalfa"]
WATER_PER_ACRE = {  # acre-ft per irrigated acre per year
    "corn": 1.40,
    "soybeans": 1.00,
    "sorghum": 0.80,
    "wheat": 0.70,
    "cotton": 1.10,
    "alfalfa": 1.80,
}


def generate_tx_extraction_dataset(
    n_counties: int = 60,
    n_years: int = 20,
    seed: int = 42,
) -> pd.DataFrame:
    """Return a per-county-per-year synthetic dataset ready for modeling.

    Columns:
        fips, year, state,
        irrigated_acres_{corn, soybeans, sorghum, wheat, cotton, alfalfa},
        saturated_thickness_m, well_density, precip_mm, grid_kwh_per_af,
        pumpage_af   <-- target
    """
    rng = np.random.default_rng(seed)

    # Stable per-county attributes
    fips = [f"48{str(rng.integers(1, 510)).zfill(3)}" for _ in range(n_counties)]
    base_thickness = rng.uniform(15, 90, n_counties)            # starting thickness
    decline_rate = rng.uniform(0.05, 0.9, n_counties)           # m/yr loss
    well_density = rng.uniform(0.5, 12, n_counties)             # wells / 100 km²
    county_corn_bias = rng.uniform(0.15, 0.65, n_counties)      # corn fraction of acres
    county_cotton_bias = rng.uniform(0.05, 0.45, n_counties)    # cotton fraction

    rows = []
    for i, f in enumerate(fips):
        total_irrigated = rng.uniform(20_000, 250_000)          # acres
        for yr_idx in range(n_years):
            year = 2005 + yr_idx
            thickness = max(5.0, base_thickness[i] - decline_rate[i] * yr_idx)

            # Crop mix drifts slightly year-to-year
            mix = _draw_crop_mix(
                rng,
                corn_bias=county_corn_bias[i] + rng.normal(0, 0.03),
                cotton_bias=county_cotton_bias[i] + rng.normal(0, 0.02),
            )
            acres_by_crop = {c: float(total_irrigated * mix[c]) for c in CROPS}

            precip = rng.normal(520, 120)  # mm/yr
            grid_kwh_per_af = rng.uniform(180, 380)  # kWh/ac-ft (lifts deeper with depletion)
            grid_kwh_per_af += max(0.0, (30 - thickness)) * 5

            crop_water = sum(acres_by_crop[c] * WATER_PER_ACRE[c] for c in CROPS)
            depletion_penalty = crop_water * 0.05 * max(0.0, (30 - thickness) / 10)
            precip_adj = -crop_water * 0.08 * (precip - 520) / 200
            noise = rng.normal(0, 0.08 * crop_water)
            pumpage = max(0.0, crop_water + depletion_penalty + precip_adj + noise)

            row = {
                "fips": f,
                "year": year,
                "state": "TX",
                "saturated_thickness_m": thickness,
                "well_density": well_density[i],
                "precip_mm": precip,
                "grid_kwh_per_af": grid_kwh_per_af,
                "pumpage_af": pumpage,
            }
            row.update({f"irrigated_acres_{c}": acres_by_crop[c] for c in CROPS})
            rows.append(row)

    return pd.DataFrame(rows)


def _draw_crop_mix(rng, corn_bias: float, cotton_bias: float) -> dict[str, float]:
    """Dirichlet-style sampler biased toward corn + cotton (Texas-ish)."""
    corn = max(0.0, corn_bias)
    cotton = max(0.0, cotton_bias)
    remaining = max(0.0, 1.0 - corn - cotton)
    others = rng.dirichlet(np.ones(4)) * remaining
    mix = {
        "corn": corn,
        "cotton": cotton,
        "soybeans": float(others[0]),
        "sorghum": float(others[1]),
        "wheat": float(others[2]),
        "alfalfa": float(others[3]),
    }
    s = sum(mix.values())
    return {k: v / s for k, v in mix.items()}


FEATURE_COLS = [
    "saturated_thickness_m",
    "well_density",
    "precip_mm",
    "grid_kwh_per_af",
    *[f"irrigated_acres_{c}" for c in CROPS],
]
TARGET_COL = "pumpage_af"


def spatial_train_test_split(
    df: pd.DataFrame, test_frac: float = 0.2, seed: int = 42
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split by FIPS (not by row) so no county leaks across sets.

    This mirrors the spec's validation strategy (§8 Day 3: "5-fold spatial CV
    on Kansas-only") — hold out whole counties, not random rows.
    """
    rng = np.random.default_rng(seed)
    counties = df["fips"].unique()
    rng.shuffle(counties)
    n_test = max(1, int(len(counties) * test_frac))
    test_counties = set(counties[:n_test])
    test = df[df["fips"].isin(test_counties)].copy()
    train = df[~df["fips"].isin(test_counties)].copy()
    return train, test
