"""Compose data/processed/baseline.parquet from all ingested sources.

One row per HPA county with every column the scenario engine expects.

Inputs (all optional — script degrades gracefully if some are missing):
  - data/processed/usgs_wells.parquet      (well inventory)
  - data/processed/usgs_gwlevels.parquet   (water-level history)
  - data/processed/hpa_boundary.parquet    (aquifer footprint)
  - data/processed/nass_irrigated_acres.parquet   (per-crop irrigated acres)
  - data/processed/iwms_water_per_acre.parquet    (per-state water rate)
  - data/processed/eia_grid_intensity.parquet     (per-state CO2/kWh)
  - data/processed/ers_revenue_per_acre.parquet   (per-crop $/acre)

Output:
  - data/processed/baseline.parquet

Fallbacks are documented inline — when a source is missing, the column is
filled from the hardcoded constants in `analytics/scenarios.py` and flagged
in `data_quality` per spec §4.
"""

from __future__ import annotations

import pandas as pd

from aquiferwatch.analytics.scenarios import (
    METHOD_EFFICIENCY,
    REVENUE_PER_ACRE,
    WATER_PER_ACRE,
)
from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)

PROCESSED = DATA_DIR / "processed"
OUTPUT = PROCESSED / "baseline.parquet"
CROPS = list(WATER_PER_ACRE.keys())
HPA_STATES = ("NE", "KS", "CO", "TX", "OK", "NM", "SD", "WY")


def _load_if_exists(name: str) -> pd.DataFrame | None:
    p = PROCESSED / name
    if not p.exists():
        log.warning("  missing: %s (falling back to constants)", name)
        return None
    return pd.read_parquet(p)


def build() -> pd.DataFrame:
    # --- 1. county frame from NASS (the only per-county source) ---
    nass = _load_if_exists("nass_irrigated_acres.parquet")
    if nass is None:
        raise RuntimeError("NASS is the spine of the baseline — can't build without it")
    # Keep most recent census year per (fips, crop)
    nass = nass.sort_values(["fips", "crop", "year"]).drop_duplicates(
        subset=["fips", "crop"], keep="last"
    )
    wide = (
        nass.pivot_table(index=["fips", "state", "state_name", "county_name"],
                         columns="crop", values="value", aggfunc="sum")
            .reset_index()
    )
    wide.columns.name = None
    # Ensure all 6 crops exist as columns (may be missing if no county has that crop)
    for c in CROPS:
        col = f"acres_{c}"
        wide[col] = wide.get(c, 0.0).fillna(0.0) if c in wide.columns else 0.0
        if c in wide.columns:
            wide = wide.drop(columns=[c])
    wide["irrigated_acres_total"] = sum(wide[f"acres_{c}"] for c in CROPS)
    wide = wide[wide["state"].isin(HPA_STATES)]
    log.info("NASS baseline: %d counties across %d states",
             len(wide), wide["state"].nunique())

    # --- 2. USGS wells — aggregate to county for data quality + thickness proxy ---
    wells = _load_if_exists("usgs_wells.parquet")
    if wells is not None:
        wells["fips"] = (
            wells["state_cd"].astype(str).str.zfill(2)
            + wells["county_cd"].astype(str).str.zfill(3)
        )
        well_agg = wells.groupby("fips").agg(
            n_wells=("site_no", "count"),
            median_well_depth_ft=("well_depth_va", "median"),
            median_altitude_ft=("alt_va", "median"),
        ).reset_index()
        wide = wide.merge(well_agg, on="fips", how="left")
    else:
        wide["n_wells"] = 0
        wide["median_well_depth_ft"] = pd.NA

    # --- 3. gwlevels: current saturated thickness + annual decline ---
    gwl = _load_if_exists("usgs_gwlevels.parquet")
    if gwl is not None and len(gwl):
        # Join measurements back to wells to get county
        gwl_sites = gwl.merge(
            wells[["site_no", "fips", "well_depth_va"]], on="site_no", how="left",
        )
        # Saturated thickness (ft) ≈ well_depth_va - depth_to_water
        gwl_sites["sat_thickness_ft"] = (
            gwl_sites["well_depth_va"] - gwl_sites["value"]
        )
        gwl_sites = gwl_sites.dropna(subset=["fips", "sat_thickness_ft"])
        # Latest per well, then median per county
        latest_per_well = (
            gwl_sites.sort_values("time").groupby("site_no").tail(1)
        )
        county_thick = (
            latest_per_well.groupby("fips")["sat_thickness_ft"].median()
            * 0.3048  # ft → m
        ).rename("saturated_thickness_m").reset_index()
        wide = wide.merge(county_thick, on="fips", how="left")
        # Annual decline = slope of per-county median thickness over the last 10 years
        gwl_sites["year"] = pd.to_datetime(gwl_sites["time"]).dt.year
        recent = gwl_sites[gwl_sites["year"] >= gwl_sites["year"].max() - 10]
        yearly = recent.groupby(["fips", "year"])["sat_thickness_ft"].median().reset_index()
        slopes = []
        for f, sub in yearly.groupby("fips"):
            if len(sub) >= 3:
                slope_ft_per_yr = _simple_slope(sub["year"].values, sub["sat_thickness_ft"].values)
                slopes.append({"fips": f, "annual_decline_m": slope_ft_per_yr * 0.3048})
        if slopes:
            wide = wide.merge(pd.DataFrame(slopes), on="fips", how="left")
        else:
            wide["annual_decline_m"] = pd.NA
    else:
        wide["saturated_thickness_m"] = pd.NA
        wide["annual_decline_m"] = pd.NA

    # --- 3b. TX TWDB override — real thickness + decline for TX HPA counties ---
    twdb = _load_if_exists("twdb_county_thickness.parquet")
    if twdb is not None and len(twdb):
        twdb_m = twdb.set_index("fips")[["saturated_thickness_m", "annual_decline_m"]]
        # Overwrite where TWDB has data (strictly better than USGS sample + fallback)
        mask = wide["fips"].isin(twdb_m.index)
        wide.loc[mask, "saturated_thickness_m"] = (
            wide.loc[mask, "fips"].map(twdb_m["saturated_thickness_m"])
        )
        wide.loc[mask, "annual_decline_m"] = (
            wide.loc[mask, "fips"].map(twdb_m["annual_decline_m"])
        )
        log.info("  TX TWDB override: %d counties get real thickness+decline", mask.sum())

    # --- 3c. NE DNR override — thickness for NE counties without USGS gwlevels data ---
    ne = _load_if_exists("ne_dnr_county_summary.parquet")
    if ne is not None and len(ne):
        ne_m = ne.set_index("fips")["saturated_thickness_m"]
        # Only overwrite where we don't already have thickness (avoid stomping gwlevels/TWDB)
        missing_thickness = wide["saturated_thickness_m"].isna()
        mask = wide["fips"].isin(ne_m.index) & missing_thickness
        wide.loc[mask, "saturated_thickness_m"] = wide.loc[mask, "fips"].map(ne_m)
        # NE is spec's "slowest depletion" state (§9 featured story 3) — default
        # decline to -0.10 m/yr (vs. HPA-wide -0.30) where we don't have real data.
        ne_missing_decline = wide["annual_decline_m"].isna() & wide["fips"].isin(ne_m.index)
        wide.loc[ne_missing_decline, "annual_decline_m"] = -0.10
        log.info("  NE DNR override: %d counties get real thickness", mask.sum())

        # Also refine well count + median well depth from NE DNR for all NE counties
        ne_cols = ne.set_index("fips")[["n_active_irr_wells", "median_total_depth_ft"]]
        ne_mask = wide["fips"].isin(ne_cols.index)
        # Prefer NE DNR's total_depth_ft over USGS monitoring-well depth
        wide.loc[ne_mask, "median_well_depth_ft"] = (
            wide.loc[ne_mask, "fips"].map(ne_cols["median_total_depth_ft"])
        )

    # Fallbacks for counties without gwlevels or TWDB data
    # Median HPA saturated thickness ~30 m (per Deines 2019), decline -0.3 m/yr.
    wide["saturated_thickness_m"] = wide["saturated_thickness_m"].fillna(30.0)
    wide["annual_decline_m"] = wide["annual_decline_m"].fillna(-0.30)
    wide["recharge_mm_yr"] = 20.0  # HPA average — USGS Water Use assessments

    # --- 4. IWMS water rates — join at (state, crop) ---
    iwms = _load_if_exists("iwms_water_per_acre.parquet")
    if iwms is not None and len(iwms):
        iwms_avg = (
            iwms.groupby(["state", "crop"])["acre_feet_per_acre"]
            .mean()
            .reset_index()
        )
        water_per_acre_by_state_crop = iwms_avg.pivot(
            index="state", columns="crop", values="acre_feet_per_acre"
        )
    else:
        water_per_acre_by_state_crop = None

    # --- 5. ERS revenue — join at crop (national, no state variation) ---
    ers = _load_if_exists("ers_revenue_per_acre.parquet")
    rev_map = dict(zip(ers["crop"], ers["gross_value_usd_per_acre"])) if ers is not None else {}

    # --- 6. EIA grid intensity — join at state ---
    eia = _load_if_exists("eia_grid_intensity.parquet")
    if eia is not None:
        wide = wide.merge(
            eia[["state", "co2_kg_per_kwh"]].rename(columns={"co2_kg_per_kwh": "grid_intensity_kg_per_kwh"}),
            on="state", how="left",
        )
    else:
        wide["grid_intensity_kg_per_kwh"] = 0.45
    wide["grid_intensity_kg_per_kwh"] = wide["grid_intensity_kg_per_kwh"].fillna(0.45)

    # --- 7. Pumping energy intensity: proxy from well depth ---
    # kWh per acre-foot ≈ 1.024 × depth_in_ft (USDA NRCS energy budget rule of thumb).
    # Convert median_well_depth_ft to kwh_per_af_pumped.
    wide["kwh_per_af_pumped"] = (
        wide["median_well_depth_ft"].fillna(200.0) * 1.024
    )

    # --- 8. Ag value per county: acres × state-specific water × revenue ---
    wide["ag_value_usd"] = sum(
        wide[f"acres_{c}"] * rev_map.get(c, REVENUE_PER_ACRE[c]) for c in CROPS
    )

    # --- 9. Pumping from crop×water_rate (state-specific where possible) ---
    pumping = pd.Series(0.0, index=wide.index)
    for c in CROPS:
        if water_per_acre_by_state_crop is not None and c in water_per_acre_by_state_crop.columns:
            rate_by_state = water_per_acre_by_state_crop[c]
            rate = wide["state"].map(rate_by_state).fillna(WATER_PER_ACRE[c])
        else:
            rate = WATER_PER_ACRE[c]
        pumping = pumping + wide[f"acres_{c}"] * rate
    wide["pumping_af_yr"] = pumping

    # --- 10. Irrigation method mix: all-center-pivot fallback ---
    # Real mix requires USDA IWMS application-method subquery — deferred.
    wide["irr_center_pivot"] = 0.85
    wide["irr_flood"] = 0.05
    wide["irr_drip"] = 0.05
    wide["irr_dryland"] = 0.05

    # --- 11. Employment per county ---
    wide["employment_fte"] = wide["ag_value_usd"] * (0.021 / 1000.0)

    # --- 12. Data quality tier per spec §4 ---
    # Metered states: KS, NE with KGS/NE DNR data (not yet); for now all
    # "modeled_high" where we have ≥ 5 wells, else "modeled_low".
    wide["data_quality"] = wide["n_wells"].fillna(0).apply(
        lambda n: "modeled_high" if n >= 5 else "modeled_low"
    )

    # Drop anything without acres to keep the frame clean.
    wide = wide[wide["irrigated_acres_total"] > 0].reset_index(drop=True)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    wide.to_parquet(OUTPUT, index=False)
    log.info("Wrote baseline: %d counties → %s", len(wide), OUTPUT)
    log.info("  columns: %s", wide.columns.tolist())
    return wide


def _simple_slope(xs, ys):
    import numpy as np
    xs = np.asarray(xs, dtype=float)
    ys = np.asarray(ys, dtype=float)
    if len(xs) < 2:
        return 0.0
    return float(((xs - xs.mean()) * (ys - ys.mean())).sum() /
                 ((xs - xs.mean()) ** 2).sum())


if __name__ == "__main__":
    build()
