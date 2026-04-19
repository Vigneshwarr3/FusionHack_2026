"""KGS Master Well Inventory — the join table across WWC5 + WRIS + WIZARD.

One row per KS well with:
    WELL_KID              — primary key
    LATITUDE / LONGITUDE  — NAD83
    COUNTY_CODE           — numeric, KS county 3-digit suffix (e.g. 23 = Cheyenne)
    WELL_DEPTH            — ft below land surface
    ELEVATION_SURFACE_NED — land surface elevation (NAVD88 ft)
    COMPLETION_DATE
    USES_OF_WATER
    WELL_STATUS           — Constructed / Plugged
    WIZARD_USGS_ID        — USGS NWIS site number when monitored by WIZARD
    WRIS_PDIV_ID          — water right point-of-diversion id (join to WIMAS)
    WWC5_INPUT_SEQ_NUMBERS — drilled-well record

This is the bridge that lets us compute saturated thickness per KS well
(WELL_DEPTH − depth_to_water) because we have WELL_DEPTH here and depth-to-water
via USGS OGC field-measurements on WIZARD_USGS_ID.

Outputs:
    data/processed/kgs_master_wells.parquet
    data/processed/kgs_county_thickness.parquet (via build_county_thickness)
"""

from __future__ import annotations

import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger
from aquiferwatch.pipeline.kgs_wimas import HPA_COUNTY_CODES, KS_COUNTY_TO_FIPS

log = get_logger(__name__)

RAW_DIR = DATA_DIR / "raw" / "kgs_master"
WELLS_OUT = DATA_DIR / "processed" / "kgs_master_wells.parquet"
COUNTY_THICKNESS_OUT = DATA_DIR / "processed" / "kgs_county_thickness.parquet"

# Map KS HPA 2-letter codes → numeric FIPS suffix for joining against COUNTY_CODE
HPA_COUNTY_NUMERIC: set[int] = {
    int(KS_COUNTY_TO_FIPS[c][2:]) for c in HPA_COUNTY_CODES if c in KS_COUNTY_TO_FIPS
}


def ingest_master_inventory() -> pd.DataFrame:
    """Parse the full 375k-well inventory, filter to HPA, write parquet."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    csvs = list(RAW_DIR.glob("*.csv"))
    if not csvs:
        log.warning("no Master Inventory CSV in %s", RAW_DIR)
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for p in csvs:
        log.info("  reading %s (%.1f MB)", p.name, p.stat().st_size / 1e6)
        df = pd.read_csv(p, low_memory=False)
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)

    # Numeric coercions
    for c in ("LATITUDE", "LONGITUDE", "WELL_DEPTH", "ELEVATION_SURFACE_NED", "COUNTY_CODE"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Build 5-digit FIPS
    df["fips"] = df["COUNTY_CODE"].apply(
        lambda c: f"20{int(c):03d}" if pd.notna(c) else None
    )
    df["is_hpa_county"] = df["COUNTY_CODE"].isin(HPA_COUNTY_NUMERIC)
    df["is_constructed"] = df["WELL_STATUS"].astype(str) == "Constructed"
    df["is_irrigation"] = df["USES_OF_WATER"].astype(str).str.contains(
        "Irrigation", case=False, na=False
    )
    df["is_monitoring"] = df["USES_OF_WATER"].astype(str).str.contains(
        "Monitor", case=False, na=False
    )
    df["has_wizard_usgs_id"] = df["WIZARD_USGS_ID"].notna()

    WELLS_OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(WELLS_OUT, index=False)
    log.info(
        "Wrote %d KS wells (%d HPA, %d with WIZARD_USGS_ID, %d HPA+WIZARD) → %s",
        len(df),
        df["is_hpa_county"].sum(),
        df["has_wizard_usgs_id"].sum(),
        (df["is_hpa_county"] & df["has_wizard_usgs_id"]).sum(),
        WELLS_OUT,
    )
    return df


def build_county_thickness() -> pd.DataFrame:
    """Per-county saturated thickness + decline for KS HPA counties.

    Strategy:
      1. Filter to HPA + constructed wells with a WIZARD_USGS_ID.
      2. Join to our USGS OGC gwlevels parquet on WIZARD_USGS_ID ↔ site_no.
      3. For wells with measurements: per-well sat_thickness = WELL_DEPTH − depth_to_water.
      4. Median across wells per county for current thickness.
      5. Per-well slope of depth_to_water over time → aggregate by median for
         decline (sign flipped since depth increasing = water dropping).
    """
    import numpy as np

    if not WELLS_OUT.exists():
        ingest_master_inventory()
    wells = pd.read_parquet(WELLS_OUT)
    wells = wells[
        wells["is_hpa_county"] & wells["is_constructed"] & wells["has_wizard_usgs_id"]
    ].copy()
    wells["WIZARD_USGS_ID"] = wells["WIZARD_USGS_ID"].astype(str).str.strip()

    gw_path = DATA_DIR / "processed" / "usgs_gwlevels.parquet"
    if not gw_path.exists():
        log.warning("USGS gwlevels not yet materialized — background task still running")
        # Fall back to just writing well inventory; thickness comes later.
        return pd.DataFrame()

    gw = pd.read_parquet(gw_path)
    # site_no is the plain USGS site number (no USGS- prefix)
    gw["site_no"] = gw["site_no"].astype(str).str.strip()
    # WIZARD_USGS_ID may be float-like or zero-padded; normalize both sides
    gw_set = set(gw["site_no"])
    wells["usgs_id_norm"] = wells["WIZARD_USGS_ID"].str.replace(r"\.0$", "", regex=True)
    joined = wells.merge(
        gw, left_on="usgs_id_norm", right_on="site_no", how="inner"
    )
    if joined.empty:
        log.warning(
            "no overlap between KGS WIZARD_USGS_ID (%d distinct) and USGS gwlevels (%d sites) "
            "— gwlevels pull may be too sparse",
            wells["usgs_id_norm"].nunique(), len(gw_set),
        )
        return pd.DataFrame()

    # value = depth-to-water in feet per our USGS OGC parser
    joined["depth_to_water_ft"] = pd.to_numeric(joined["value"], errors="coerce")
    joined["sat_thickness_ft"] = joined["WELL_DEPTH"] - joined["depth_to_water_ft"]
    joined["sat_thickness_m"] = joined["sat_thickness_ft"] * 0.3048
    joined["year"] = pd.to_datetime(joined["time"], errors="coerce", utc=True).dt.year
    joined = joined.dropna(subset=["fips", "sat_thickness_m", "year", "depth_to_water_ft"])

    # Current: latest measurement per well, median per county
    latest = joined.sort_values("year").groupby("WIZARD_USGS_ID").tail(1)
    current = (
        latest.groupby("fips")["sat_thickness_m"].median()
        .rename("saturated_thickness_m").reset_index()
    )

    # Decline: per-well slope of depth_to_water over 20y, flip sign (water table elevation = −depth)
    max_year = int(joined["year"].max())
    recent = joined[joined["year"] >= max_year - 20]

    def _well_slope(sub: pd.DataFrame) -> float | None:
        if len(sub) < 3 or sub["year"].max() - sub["year"].min() < 5:
            return None
        x = sub["year"].values.astype(float)
        y = sub["depth_to_water_ft"].values.astype(float)
        denom = ((x - x.mean()) ** 2).sum()
        if denom == 0:
            return None
        slope = ((x - x.mean()) * (y - y.mean())).sum() / denom
        # depth rising = water table falling → return −slope (m/yr)
        return float(-slope * 0.3048)

    per_well = recent.groupby(["fips", "WIZARD_USGS_ID"]).apply(_well_slope, include_groups=False)
    per_well = per_well.dropna().reset_index(name="annual_decline_m")
    decline = per_well.groupby("fips")["annual_decline_m"].median().reset_index()

    out = current.merge(decline, on="fips", how="left")
    out["source"] = "kgs_master_via_usgs"
    COUNTY_THICKNESS_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(COUNTY_THICKNESS_OUT, index=False)
    log.info(
        "Wrote KS county thickness (%d counties, joined %d wells × %d measurements) → %s",
        len(out), joined["WIZARD_USGS_ID"].nunique(), len(joined),
        COUNTY_THICKNESS_OUT,
    )
    return out


if __name__ == "__main__":
    ingest_master_inventory()
    build_county_thickness()
