"""Texas Water Development Board ingest.

Texas is the cautionary tale (spec §4): rule of capture → most counties lack
metered extraction. We pull well registrations + GCD boundaries + partial
reported pumpage; the gap gets closed by `analytics.extraction_imputation`.

**Programmatic access is partial.** TWDB's web interface
(`twdb.texas.gov/groundwater/data/gwdbrpt.asp`) generates reports on demand.
ArcGIS REST services (`maps.twdb.texas.gov/arcgis/rest/services/`) publish
spatial layers. Since we couldn't reach their bulk endpoints from this
environment (DNS blocks), the fallback is manual download:

  1. TWDB GWDB Groundwater Database:
     https://www.twdb.texas.gov/groundwater/data/gwdbrpt.asp
     Run "Wells by County" for the 45 HPA-overlapping TX counties, export CSV
     to `data/raw/twdb/wells_<county>.csv`.

  2. GCD boundaries shapefile:
     https://www.twdb.texas.gov/mapping/gisdata.asp → download gcd.zip
     Extract to `data/raw/twdb/gcd/`.

  3. Run `python -m aquiferwatch.pipeline.tx_twdb` to produce parquet.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)

RAW_DIR = DATA_DIR / "raw" / "twdb"
EXTRACTED = RAW_DIR / "extracted" / "GWDBDownload"
WELLS_OUT = DATA_DIR / "processed" / "twdb_wells.parquet"
LEVELS_OUT = DATA_DIR / "processed" / "twdb_water_levels.parquet"
COUNTY_AGG_OUT = DATA_DIR / "processed" / "twdb_county_thickness.parquet"


def _tx_county_to_fips() -> dict[str, str]:
    """Build TX county-name → 5-digit FIPS from the NASS parquet we already have."""
    nass_path = DATA_DIR / "processed" / "nass_irrigated_acres.parquet"
    if not nass_path.exists():
        log.warning("NASS parquet missing — can't build TX FIPS map")
        return {}
    nass = pd.read_parquet(nass_path)
    tx = nass[nass["state"] == "TX"][["county_name", "fips"]].drop_duplicates()
    # NASS names are UPPER, TWDB is Title Case — normalize on upper for the key
    return {n.upper(): f for n, f in zip(tx["county_name"], tx["fips"])}


def ingest_tx_wells() -> pd.DataFrame:
    """Parse TWDB WellMain.txt → Ogallala wells in TX HPA counties only."""
    wm_path = EXTRACTED / "WellMain.txt"
    if not wm_path.exists():
        log.warning("no WellMain.txt in %s — extract GWDBDownload.zip first", EXTRACTED)
        return pd.DataFrame()

    cols = [
        "StateWellNumber", "County", "AquiferCode", "Aquifer",
        "LatitudeDD", "LongitudeDD", "WellDepth",
        "LandSurfaceElevation", "DrillingYear",
        "WellUse", "GCD", "GMA",
        "USGSSiteNumber", "WaterLevelStatus",
    ]
    log.info("  reading WellMain.txt (~60 MB, 142k rows)...")
    df = pd.read_csv(
        wm_path, sep="|", usecols=cols, low_memory=False, encoding="latin-1",
    )
    # Filter to Ogallala aquifer (includes Ogallala/Rita Blanca combos via code 121OG*)
    df = df[df["Aquifer"].astype(str).str.contains("Ogallala", case=False, na=False)]

    for c in ("LatitudeDD", "LongitudeDD", "WellDepth", "LandSurfaceElevation", "DrillingYear"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Map county name to 5-digit FIPS
    fips_map = _tx_county_to_fips()
    df["fips"] = df["County"].str.upper().map(fips_map)

    WELLS_OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(WELLS_OUT, index=False)
    log.info(
        "Wrote %d TX Ogallala wells (%d counties, %d FIPS-resolved) → %s",
        len(df), df["County"].nunique(), df["fips"].notna().sum(), WELLS_OUT,
    )
    return df


def ingest_tx_water_levels(chunk_size: int = 200_000) -> pd.DataFrame:
    """Parse WaterLevelsMajor.txt → Ogallala measurements only.

    The file is ~321 MB, 1.77M rows. We stream in chunks, filter on Aquifer,
    keep only rows whose StateWellNumber appears in our cached wells parquet.
    """
    wl_path = EXTRACTED / "WaterLevelsMajor.txt"
    if not wl_path.exists():
        log.warning("no WaterLevelsMajor.txt in %s", EXTRACTED)
        return pd.DataFrame()

    if not WELLS_OUT.exists():
        ingest_tx_wells()
    wells = pd.read_parquet(WELLS_OUT)
    wells_set = set(wells["StateWellNumber"].astype(str))
    well_depth = wells.set_index("StateWellNumber")["WellDepth"].to_dict()

    keep_cols = [
        "StateWellNumber", "County", "Aquifer", "Status",
        "MeasurementDate", "DepthFromLSD", "LandElevation", "WaterElevation",
        "MeasuringAgency", "MethodOfMeasurement",
    ]
    frames: list[pd.DataFrame] = []
    total_rows = 0
    log.info("  streaming WaterLevelsMajor.txt (1.77M rows, ~321 MB)...")
    for chunk in pd.read_csv(
        wl_path, sep="|", usecols=keep_cols, chunksize=chunk_size,
        low_memory=False, encoding="latin-1",
    ):
        chunk = chunk[chunk["StateWellNumber"].astype(str).isin(wells_set)]
        if not chunk.empty:
            frames.append(chunk)
        total_rows += chunk_size
        if total_rows % 1_000_000 == 0:
            log.info("    scanned %dM rows, kept %d",
                     total_rows // 1_000_000,
                     sum(len(f) for f in frames))

    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df["MeasurementDate"] = pd.to_datetime(df["MeasurementDate"], errors="coerce")
    df["DepthFromLSD"] = pd.to_numeric(df["DepthFromLSD"], errors="coerce")
    df["WellDepth"] = df["StateWellNumber"].astype(str).map(
        {str(k): v for k, v in well_depth.items()}
    )
    # Saturated thickness ≈ WellDepth − DepthFromLSD (both in ft)
    df["sat_thickness_ft"] = df["WellDepth"] - df["DepthFromLSD"]
    df["sat_thickness_m"] = df["sat_thickness_ft"] * 0.3048

    df.to_parquet(LEVELS_OUT, index=False)
    log.info("Wrote %d Ogallala water-level measurements → %s", len(df), LEVELS_OUT)
    return df


def build_county_thickness() -> pd.DataFrame:
    """Roll up water levels to per-county current thickness + 10y decline.

    Decline methodology: **per-well slope of water-table elevation, then
    aggregate**. Earlier code fit slopes of median thickness across wells per
    year and produced absurd positive slopes for known-depleting counties
    (Dallam showed +5.96 m/yr). Cause: well sampling shifts over time —
    counties drill deeper new wells as water drops, so the across-wells-median
    of (WellDepth − DepthToWater) inflates spuriously. Per-well slope of
    WaterElevation is robust to that: each well is its own baseline.
    """
    import numpy as np

    wells = pd.read_parquet(WELLS_OUT)
    levels = pd.read_parquet(LEVELS_OUT)
    levels = levels.merge(wells[["StateWellNumber", "fips"]], on="StateWellNumber")
    levels["WaterElevation"] = pd.to_numeric(levels["WaterElevation"], errors="coerce")
    levels = levels.dropna(
        subset=["fips", "sat_thickness_m", "MeasurementDate", "WaterElevation"]
    )
    levels["year"] = levels["MeasurementDate"].dt.year

    # --- Current saturated thickness: latest measurement per well, median per county
    latest_per_well = (
        levels.sort_values("MeasurementDate").groupby("StateWellNumber").tail(1)
    )
    current = (
        latest_per_well.groupby("fips")["sat_thickness_m"].median()
        .rename("saturated_thickness_m").reset_index()
    )

    # --- Per-well decline over last 20 years, aggregate to county ---
    max_year = int(levels["year"].max())
    recent = levels[levels["year"] >= max_year - 20]

    def _well_slope(sub: pd.DataFrame) -> float | None:
        # Require at least 3 measurements spanning ≥5 years
        if len(sub) < 3 or sub["year"].max() - sub["year"].min() < 5:
            return None
        x = sub["year"].values.astype(float)
        y = sub["WaterElevation"].values.astype(float)
        # slope = d(WaterElevation)/d(year); negative = water table falling
        denom = ((x - x.mean()) ** 2).sum()
        if denom == 0:
            return None
        return float(((x - x.mean()) * (y - y.mean())).sum() / denom)

    well_slopes = recent.groupby(["fips", "StateWellNumber"]).apply(_well_slope, include_groups=False)
    well_slopes = well_slopes.dropna().reset_index(name="slope_ft_per_yr")

    # County decline = median per-well slope × ft→m; sign is preserved
    county_decline = (
        well_slopes.groupby("fips")["slope_ft_per_yr"].median() * 0.3048
    ).rename("annual_decline_m").reset_index()

    out = current.merge(county_decline, on="fips", how="left")
    out["source"] = "twdb"
    COUNTY_AGG_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(COUNTY_AGG_OUT, index=False)
    log.info("Wrote TX county thickness (%d counties) → %s", len(out), COUNTY_AGG_OUT)
    return out


def ingest_tx_gcd_boundaries() -> None:
    """GCD shapefile — TWDB_Groundwater.shp (really the well-points shapefile,
    not GCDs). Kept for well density + interactive map overlays."""
    shp = RAW_DIR / "extracted" / "TWDB_Groundwater.shp"
    if not shp.exists():
        log.warning("no shapefile at %s", shp)
        return
    import geopandas as gpd

    gdf = gpd.read_file(shp).to_crs("EPSG:4326")
    out = DATA_DIR / "processed" / "twdb_well_points.parquet"
    gdf.to_parquet(out)
    log.info("Wrote %d TWDB well points → %s", len(gdf), out)


if __name__ == "__main__":
    ingest_tx_wells()
    ingest_tx_water_levels()
    build_county_thickness()
    ingest_tx_gcd_boundaries()
