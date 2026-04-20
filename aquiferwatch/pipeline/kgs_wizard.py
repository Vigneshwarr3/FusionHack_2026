"""KGS WIZARD — Kansas Geological Survey Water-Level Database.

The canonical Kansas water-level source. Exports come as paired files:
  sites<timestamp>.csv    — one row per well (metadata)
  wlevel<timestamp>.csv   — one row per measurement (time series)

The KGS WIZARD portal caps exports at ~3,000 wells per download, so expect
a handful of paired files. Parser unions them all by USGS_ID.

Key site columns:
  USGS_ID, STATE_CODE, COUNTY_CODE, LATITUDE, LONGITUDE,
  DEPTH_OF_WELL, DEPTH_TO_BEDROCK, LAND_SURFACE_ALTITUDE,
  GROUNDWATER_MGMT_DISTRICT, USE_OF_WATER_PRIMARY, WELL_KID

Key level columns:
  USGS_ID, MEASUREMENT_DATE_AND_TIME, DEPTH_TO_WATER, METHOD, AGENCY

Outputs:
  data/processed/kgs_wizard_sites.parquet
  data/processed/kgs_wizard_levels.parquet
  data/processed/kgs_wizard_county_thickness.parquet
"""

from __future__ import annotations

import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)

RAW_DIR = DATA_DIR / "raw" / "kgs_wimas" / "WIZARD"
SITES_OUT = DATA_DIR / "processed" / "kgs_wizard_sites.parquet"
LEVELS_OUT = DATA_DIR / "processed" / "kgs_wizard_levels.parquet"
COUNTY_OUT = DATA_DIR / "processed" / "kgs_wizard_county_thickness.parquet"


def ingest_all() -> None:
    sites_files = sorted(RAW_DIR.glob("sites*.csv"))
    wlevel_files = sorted(RAW_DIR.glob("wlevel*.csv"))
    if not sites_files:
        log.warning("no sites*.csv in %s", RAW_DIR)
        return

    log.info("found %d site files, %d wlevel files", len(sites_files), len(wlevel_files))

    site_frames = [pd.read_csv(p, low_memory=False, encoding="latin-1") for p in sites_files]
    sites = pd.concat(site_frames, ignore_index=True)
    # Column names have a trailing-space on WELL_KID in the export
    sites.columns = [c.strip() for c in sites.columns]
    sites = sites.drop_duplicates(subset=["USGS_ID"], keep="last")

    for c in ("LATITUDE", "LONGITUDE", "STATE_CODE", "COUNTY_CODE",
              "DEPTH_OF_WELL", "DEPTH_OF_HOLE", "DEPTH_TO_BEDROCK",
              "LAND_SURFACE_ALTITUDE", "INV_WATER_LEVEL",
              "GROUNDWATER_MGMT_DISTRICT"):
        if c in sites.columns:
            sites[c] = pd.to_numeric(sites[c], errors="coerce")

    sites["fips"] = (
        sites["STATE_CODE"].astype("Int64").astype(str).str.zfill(2)
        + sites["COUNTY_CODE"].astype("Int64").astype(str).str.zfill(3)
    )
    # Coerce object columns to string to avoid parquet type errors
    for col in sites.columns:
        if sites[col].dtype == "object":
            sites[col] = sites[col].astype(str)

    SITES_OUT.parent.mkdir(parents=True, exist_ok=True)
    sites.to_parquet(SITES_OUT, index=False)
    log.info("Wrote %d WIZARD sites across %d counties → %s",
             len(sites), sites["fips"].nunique(), SITES_OUT)

    if wlevel_files:
        wl_frames = [pd.read_csv(p, low_memory=False, encoding="latin-1") for p in wlevel_files]
        wl = pd.concat(wl_frames, ignore_index=True)
        wl["DEPTH_TO_WATER"] = pd.to_numeric(wl["DEPTH_TO_WATER"], errors="coerce")
        wl["MEASUREMENT_DATE_AND_TIME"] = pd.to_datetime(
            wl["MEASUREMENT_DATE_AND_TIME"], errors="coerce"
        )
        wl = wl.dropna(subset=["USGS_ID", "DEPTH_TO_WATER", "MEASUREMENT_DATE_AND_TIME"])
        wl = wl.drop_duplicates(
            subset=["USGS_ID", "MEASUREMENT_DATE_AND_TIME"], keep="last"
        )
        for col in wl.columns:
            if wl[col].dtype == "object":
                wl[col] = wl[col].astype(str)
        wl.to_parquet(LEVELS_OUT, index=False)
        log.info("Wrote %d WIZARD measurements → %s", len(wl), LEVELS_OUT)


def build_county_thickness() -> pd.DataFrame:
    """Per-county thickness (WellDepth − DepthToWater at latest measurement)
    + per-well decline slope, aggregated by median across wells."""
    if not SITES_OUT.exists() or not LEVELS_OUT.exists():
        log.warning("run ingest_all first")
        return pd.DataFrame()

    sites = pd.read_parquet(SITES_OUT)
    wl = pd.read_parquet(LEVELS_OUT)
    wl = wl.merge(
        sites[["USGS_ID", "fips", "DEPTH_OF_WELL", "LAND_SURFACE_ALTITUDE"]],
        on="USGS_ID", how="left",
    )
    wl = wl.dropna(subset=["fips", "DEPTH_OF_WELL", "DEPTH_TO_WATER"])
    wl["sat_thickness_ft"] = wl["DEPTH_OF_WELL"] - wl["DEPTH_TO_WATER"]
    wl["sat_thickness_m"] = wl["sat_thickness_ft"] * 0.3048
    wl["year"] = pd.to_datetime(wl["MEASUREMENT_DATE_AND_TIME"], errors="coerce").dt.year

    # Current thickness: latest per well, median per county
    latest = wl.sort_values("MEASUREMENT_DATE_AND_TIME").groupby("USGS_ID").tail(1)
    current = (
        latest.groupby("fips")["sat_thickness_m"].median()
        .rename("saturated_thickness_m").reset_index()
    )

    # Decline: per-well slope of DEPTH_TO_WATER (rising depth = water falling)
    # Flip sign so negative = depletion.
    import numpy as np
    max_year = int(wl["year"].max())
    recent = wl[wl["year"] >= max_year - 20]

    def _slope(sub: pd.DataFrame) -> float | None:
        sub = sub.dropna(subset=["year", "DEPTH_TO_WATER"])
        if len(sub) < 3 or sub["year"].max() - sub["year"].min() < 5:
            return None
        x = sub["year"].values.astype(float)
        y = sub["DEPTH_TO_WATER"].values.astype(float)
        denom = ((x - x.mean()) ** 2).sum()
        if denom == 0:
            return None
        slope_ft_per_yr = ((x - x.mean()) * (y - y.mean())).sum() / denom
        return float(-slope_ft_per_yr * 0.3048)

    per_well = recent.groupby(["fips", "USGS_ID"]).apply(_slope, include_groups=False)
    per_well = per_well.dropna().reset_index(name="annual_decline_m")
    decline = per_well.groupby("fips")["annual_decline_m"].median().reset_index()

    out = current.merge(decline, on="fips", how="left")
    out["n_wells"] = latest.groupby("fips")["USGS_ID"].nunique().reindex(out["fips"]).values
    out["source"] = "kgs_wizard"
    COUNTY_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(COUNTY_OUT, index=False)
    log.info("Wrote KGS WIZARD county thickness (%d counties) → %s", len(out), COUNTY_OUT)
    return out


if __name__ == "__main__":
    ingest_all()
    build_county_thickness()
