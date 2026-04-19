"""KGS HPA geoportal — bedrock elevation + extent classification.

Source: Kansas Geoportal "High Plains Aquifer" dataset
(https://www.kansasgis.org/). Three tables:

  extent.csv  (88 rows)        — HPA density polygon *attributes*
                                 (DESCRIPTION ∈ {outcrop, little, some}).
                                 Geometry NOT in the CSV export.
  base.csv    (1,902 rows)     — bedrock-elevation contour *line* attributes
                                 (BED_ELEV in feet). Geometry NOT in CSV.
  bedrock_wells.csv (37,026)   — wells that reached bedrock, with FIPS +
                                 surface elevation + well depth + bed_depth
                                 + bed_elev. This is the usable table.

The bedrock_wells table lets us compute the aquifer floor at thousands of
known points, aggregate to county, and combine with water-table elevation
(from USGS OGC field-measurements) to get true saturated thickness:

    saturated_thickness = (surf_elev − depth_to_water) − bed_elev

This is the method Deines et al. 2019 use for the HPA depletion paper and is
the preferred approach over (WellDepth − DepthToWater) because it's
independent of drilling decisions.

Outputs:
    data/processed/kgs_hpa_extent.parquet
    data/processed/kgs_hpa_base_contours.parquet
    data/processed/kgs_bedrock_wells.parquet
    data/processed/kgs_county_bedrock.parquet  (per-county median bed_elev + surf_elev)
"""

from __future__ import annotations

import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)

RAW_DIR = DATA_DIR / "raw" / "kgs_highplains_aquifer"
EXTENT_OUT = DATA_DIR / "processed" / "kgs_hpa_extent.parquet"
BASE_OUT = DATA_DIR / "processed" / "kgs_hpa_base_contours.parquet"
WELLS_OUT = DATA_DIR / "processed" / "kgs_bedrock_wells.parquet"
COUNTY_OUT = DATA_DIR / "processed" / "kgs_county_bedrock.parquet"


def ingest_all() -> dict[str, pd.DataFrame]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out: dict[str, pd.DataFrame] = {}

    extent_path = RAW_DIR / "High_Plains_Aquifer_extent.csv"
    if extent_path.exists():
        df = pd.read_csv(extent_path)
        EXTENT_OUT.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(EXTENT_OUT, index=False)
        log.info("Wrote %d HPA extent polygons (attribute-only) → %s",
                 len(df), EXTENT_OUT)
        out["extent"] = df

    base_path = RAW_DIR / "High_Plains_Aquifer_base.csv"
    if base_path.exists():
        df = pd.read_csv(base_path)
        df["BED_ELEV"] = pd.to_numeric(df["BED_ELEV"], errors="coerce")
        df.to_parquet(BASE_OUT, index=False)
        log.info("Wrote %d HPA bedrock contour lines → %s", len(df), BASE_OUT)
        out["base"] = df

    wells_path = RAW_DIR / "High_Plains_Aquifer_bedrock_wells.csv"
    if wells_path.exists():
        df = pd.read_csv(wells_path, low_memory=False)
        # Numeric coercions — several columns have occasional strings
        for c in ("LAT", "LONG_", "SURF_ELEV", "WELL_DEPTH", "BED_DEPTH", "BED_ELEV"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["FIPS"] = df["FIPS"].astype(str).str.strip().str.zfill(5)
        df.to_parquet(WELLS_OUT, index=False)
        log.info("Wrote %d bedrock wells → %s", len(df), WELLS_OUT)
        out["bedrock_wells"] = df

        # Per-county rollup: median bedrock + surface elevation
        agg = df.groupby("FIPS").agg(
            n_bedrock_wells=("OBJECTID", "count"),
            median_surf_elev_ft=("SURF_ELEV", "median"),
            median_bed_elev_ft=("BED_ELEV", "median"),
            median_well_depth_ft=("WELL_DEPTH", "median"),
            median_bed_depth_ft=("BED_DEPTH", "median"),
        ).reset_index().rename(columns={"FIPS": "fips"})
        # Implied thickness (no water measurement): SURF − BED (full aquifer thickness,
        # assumes water table at surface — overestimate). Kept as a sanity-check column.
        agg["implied_full_thickness_ft"] = (
            agg["median_surf_elev_ft"] - agg["median_bed_elev_ft"]
        )
        agg["implied_full_thickness_m"] = agg["implied_full_thickness_ft"] * 0.3048
        agg.to_parquet(COUNTY_OUT, index=False)
        log.info("Wrote KS county bedrock summary (%d counties) → %s",
                 len(agg), COUNTY_OUT)
        out["county_bedrock"] = agg

    return out


def compute_thickness_from_water_levels() -> pd.DataFrame:
    """Fold USGS water-level measurements against bedrock elevation.

    Returns per-county median saturated thickness + per-well slope of water
    elevation over 20 years → county median decline.
    """
    if not WELLS_OUT.exists():
        raise RuntimeError("run ingest_all first")
    wells = pd.read_parquet(WELLS_OUT)

    gw_path = DATA_DIR / "processed" / "usgs_gwlevels.parquet"
    if not gw_path.exists():
        log.warning("usgs_gwlevels.parquet not yet materialized")
        return pd.DataFrame()

    # Wells without an OTHER_ID can't join to USGS; the ones with USGS site ids
    # in OTHER_ID can be joined. Primary cross-reference path is through the
    # Master Well Inventory though — this is a best-effort alternate.
    gw = pd.read_parquet(gw_path)
    gw["site_no"] = gw["site_no"].astype(str).str.strip()
    gw["depth_to_water_ft"] = pd.to_numeric(gw["value"], errors="coerce")
    gw["year"] = pd.to_datetime(gw["time"], errors="coerce", utc=True).dt.year

    # Per-county: median water level from USGS-known wells in that county
    # (approximated by spatial-within via FIPS when available)
    # This is kept coarse for now; the primary path is via Master Inventory.
    log.info("compute_thickness_from_water_levels: stub path; primary join lives in kgs_master")
    return gw


if __name__ == "__main__":
    ingest_all()
