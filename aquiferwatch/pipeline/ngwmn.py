"""USGS NGWMN (National Ground-Water Monitoring Network) ingest.

The NGWMN portal download interface only lets you select ~100 wells per
download, so users end up with a pile of `data(1).zip`, `data(2).zip` etc.
in a staging directory. This module:

  1. Scans `data/raw/ngwmn/staging/` for every `data(*).zip`
  2. Opens each zip, reads SITE_INFO.csv to detect county via (StateCd, CountyCd)
  3. Concatenates into per-source parquets (dedup by site)

Outputs (all in data/processed/):
  ngwmn_sites.parquet        — one row per well (deduped across zips)
  ngwmn_water_levels.parquet — all measurements (deduped by SiteId + timestamp)
  ngwmn_county_thickness.parquet — per-county median saturated thickness + decline

Downstream: compose_baseline uses ngwmn_county_thickness as the primary KS/NE/TX
source and as any-state gap-fill where earlier pipelines didn't cover a county.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)

STAGING_DIR = DATA_DIR / "raw" / "ngwmn" / "staging"
PROCESSED_DIR = DATA_DIR / "processed"
SITES_OUT = PROCESSED_DIR / "ngwmn_sites.parquet"
LEVELS_OUT = PROCESSED_DIR / "ngwmn_water_levels.parquet"
COUNTY_OUT = PROCESSED_DIR / "ngwmn_county_thickness.parquet"


def ingest_all() -> None:
    """Recursively scan data/raw/ngwmn/ for zips, build combined parquets.

    Accepts any layout the user wants — flat, per-state subfolders
    (NE/data(1).zip, KS/data(2).zip, ...), or staging/. The parser
    recovers county identity from each zip's SITE_INFO.csv regardless.
    """
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    zips = sorted(STAGING_DIR.parent.rglob("data*.zip"))
    if not zips:
        log.warning("no data*.zip files found under %s", STAGING_DIR.parent)
        return
    log.info("found %d zips across %d folders",
             len(zips), len(set(z.parent for z in zips)))

    site_frames: list[pd.DataFrame] = []
    level_frames: list[pd.DataFrame] = []

    for zp in zips:
        log.info("  processing %s", zp.name)
        try:
            with zipfile.ZipFile(zp) as zf:
                names = zf.namelist()
                site_name = next((n for n in names if n.upper().endswith("SITE_INFO.CSV")), None)
                wl_name = next((n for n in names if n.upper().endswith("WATERLEVEL.CSV")), None)
                if not site_name or not wl_name:
                    log.warning("    skipping — missing SITE_INFO or WATERLEVEL")
                    continue
                with zf.open(site_name) as f:
                    sites = pd.read_csv(f, low_memory=False)
                with zf.open(wl_name) as f:
                    levels = pd.read_csv(f, low_memory=False)

            # Detect county from first row
            if "StateCd" in sites.columns and "CountyCd" in sites.columns:
                state = sites["StateCd"].iloc[0]
                county = sites["CountyCd"].iloc[0]
                fips = f"{int(state):02d}{int(county):03d}"
                log.info(
                    "    %s → %d sites, %d measurements (FIPS %s, %s)",
                    zp.name, len(sites), len(levels), fips,
                    sites["CountyNm"].iloc[0] if "CountyNm" in sites.columns else "?",
                )
            sites["source_zip"] = zp.name
            levels["source_zip"] = zp.name
            site_frames.append(sites)
            level_frames.append(levels)
        except Exception as e:
            log.warning("    %s failed: %s", zp.name, e)
            continue

    if not site_frames:
        log.warning("no usable zips processed")
        return

    sites = pd.concat(site_frames, ignore_index=True)
    levels = pd.concat(level_frames, ignore_index=True)

    # Across 100+ zips, NGWMN exports inconsistent types (some numeric,
    # some object, some all-NaN). Coerce every object-dtype column to
    # string so parquet serialization doesn't choke.
    numeric_cols = {
        "DecLatVa", "DecLongVa", "HorzAcy", "AltVa", "AltAcy", "WellDepth",
        "StateCd", "CountyCd", "Original Value", "Accuracy Value",
        "Depth to Water Below Land Surface in ft.",
        "Water level in feet relative to NAVD88",
    }
    for col in sites.columns:
        if col in numeric_cols:
            sites[col] = pd.to_numeric(sites[col], errors="coerce")
        elif sites[col].dtype == "object":
            sites[col] = sites[col].astype(str)
    for col in levels.columns:
        if col in numeric_cols:
            levels[col] = pd.to_numeric(levels[col], errors="coerce")
        elif levels[col].dtype == "object":
            levels[col] = levels[col].astype(str)

    # Build a canonical SiteId in WATERLEVEL too: WATERLEVEL uses AgencyCd+SiteNo
    # while SITE_INFO has a composite SiteId column. Normalize to AgencyCd-SiteNo
    # across both so joins work.
    if "AgencyCd" in levels.columns and "SiteNo" in levels.columns:
        levels["SiteId"] = levels["AgencyCd"].astype(str) + "-" + levels["SiteNo"].astype(str)
    if "AgencyCd" in sites.columns and "SiteNo" in sites.columns:
        sites["SiteId"] = sites["AgencyCd"].astype(str) + "-" + sites["SiteNo"].astype(str)

    # Dedup — same site may appear in multiple zips if user downloaded overlapping selections
    sites = sites.drop_duplicates(subset=["SiteId"], keep="last")
    if "Time" in levels.columns:
        levels = levels.drop_duplicates(subset=["SiteId", "Time"], keep="last")
    elif "DateTime" in levels.columns:
        levels = levels.drop_duplicates(subset=["SiteId", "DateTime"], keep="last")

    # Normalize the FIPS column for downstream joins. Coerce defensively —
    # across 100+ zips a handful have StateCd/CountyCd as strings due to
    # inconsistent NGWMN export encoding.
    if "StateCd" in sites.columns and "CountyCd" in sites.columns:
        st = pd.to_numeric(sites["StateCd"], errors="coerce")
        co = pd.to_numeric(sites["CountyCd"], errors="coerce")
        valid = st.notna() & co.notna()
        sites["fips"] = pd.NA
        sites.loc[valid, "fips"] = (
            st[valid].astype(int).astype(str).str.zfill(2)
            + co[valid].astype(int).astype(str).str.zfill(3)
        )
        bad = (~valid).sum()
        if bad:
            log.warning("  %d rows had non-numeric StateCd/CountyCd — dropped from FIPS", bad)

    # Numeric coercions
    for c in ("DecLatVa", "DecLongVa", "AltVa", "WellDepth"):
        if c in sites.columns:
            sites[c] = pd.to_numeric(sites[c], errors="coerce")

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    sites.to_parquet(SITES_OUT, index=False)
    levels.to_parquet(LEVELS_OUT, index=False)
    log.info(
        "Wrote %d unique sites across %d counties → %s",
        len(sites), sites["fips"].nunique() if "fips" in sites.columns else 0, SITES_OUT,
    )
    log.info("Wrote %d water-level measurements → %s", len(levels), LEVELS_OUT)


def build_county_thickness() -> pd.DataFrame:
    """Median saturated thickness + decline per county, from NGWMN sites+levels.

    Computation per site:
      sat_thickness = WellDepth − depth_to_water_at_latest_measurement
      decline       = per-well slope of (Altitude − depth_to_water) vs year

    Then aggregate by county (median across wells).
    """
    if not SITES_OUT.exists() or not LEVELS_OUT.exists():
        log.warning("run ingest_all first")
        return pd.DataFrame()

    sites = pd.read_parquet(SITES_OUT)
    levels = pd.read_parquet(LEVELS_OUT)

    # NGWMN WATERLEVEL has columns like SiteId, Time, Original value, Depth to
    # Water Below Land Surface, Water Level in feet relative to NAVD88, ...
    # Detect the depth column.
    depth_col = _pick_col(levels, [
        "Depth to Water Below Land Surface in ft.",
        "DepthToWaterLS", "DepthToWaterBelowLandSurface",
        "Depth to Water Below Land Surface",
        "Depth to water level, feet below land surface",
        "DepthBLS", "LandSurfaceWlValue",
    ])
    value_col = _pick_col(levels, ["Original value", "OriginalValue", "Value"])
    time_col = _pick_col(levels, ["Time", "DateTime", "Date"])

    if depth_col is None and value_col is None:
        log.warning("could not find depth-to-water column in WATERLEVEL; cols: %s",
                    levels.columns.tolist())
        return pd.DataFrame()

    depth = depth_col or value_col
    levels = levels.dropna(subset=[depth, "SiteId"]).copy()
    levels[depth] = pd.to_numeric(levels[depth], errors="coerce")
    levels = levels.dropna(subset=[depth])
    if time_col:
        # Mixed time zones across zips — force UTC to produce a uniform dtype
        t = pd.to_datetime(levels[time_col], errors="coerce", utc=True)
        levels["year"] = t.dt.year
    else:
        levels["year"] = pd.NA

    # Join site metadata
    j = levels.merge(
        sites[["SiteId", "fips", "WellDepth", "AltVa"]], on="SiteId", how="left"
    )
    j = j.dropna(subset=["fips", "WellDepth"])
    j["sat_thickness_ft"] = j["WellDepth"] - j[depth]
    j["sat_thickness_m"] = j["sat_thickness_ft"] * 0.3048

    # Current: latest per well, median per county
    latest = j.sort_values("year").groupby("SiteId").tail(1)
    current = (
        latest.groupby("fips")["sat_thickness_m"].median()
        .rename("saturated_thickness_m").reset_index()
    )

    # Decline: per-well slope of depth (positive slope = water table falling),
    # flip sign so negative = depletion, consistent with scenarios schema.
    import numpy as np

    def _slope(sub: pd.DataFrame) -> float | None:
        sub = sub.dropna(subset=["year", depth])
        if len(sub) < 3 or sub["year"].max() - sub["year"].min() < 5:
            return None
        x = sub["year"].values.astype(float)
        y = sub[depth].values.astype(float)
        denom = ((x - x.mean()) ** 2).sum()
        if denom == 0:
            return None
        slope = ((x - x.mean()) * (y - y.mean())).sum() / denom
        # depth rising (positive slope of depth) = water falling, so return negative
        return float(-slope * 0.3048)

    per_well = j.groupby(["fips", "SiteId"]).apply(_slope, include_groups=False)
    per_well = per_well.dropna().reset_index(name="annual_decline_m")
    decline = per_well.groupby("fips")["annual_decline_m"].median().reset_index()

    out = current.merge(decline, on="fips", how="left")
    out["n_wells"] = latest.groupby("fips")["SiteId"].nunique().reindex(out["fips"]).values
    out["source"] = "ngwmn"
    out.to_parquet(COUNTY_OUT, index=False)
    log.info("Wrote NGWMN county thickness (%d counties) → %s", len(out), COUNTY_OUT)
    return out


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


if __name__ == "__main__":
    ingest_all()
    build_county_thickness()
