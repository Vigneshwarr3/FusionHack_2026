"""Nebraska DNR ingest.

Nebraska is the positive story (spec §9 featured story 3): best-instrumented
non-Kansas state, strong NRD conservation districts, slowest depletion.

**Manual download required** for NE DNR — wells and NRD use reports are
published as annual PDF/CSV at https://dnrdata.dnr.ne.gov/wellssql/ and
individual NRD websites.

  1. Registered wells CSV → `data/raw/ne_dnr/registered_wells.csv`
  2. NRD annual use reports → `data/raw/ne_dnr/nrd_use_<year>.csv`
"""

from __future__ import annotations

import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)

RAW_DIR = DATA_DIR / "raw" / "ne_dnr"
WELLS_OUT = DATA_DIR / "processed" / "ne_dnr_wells.parquet"
USE_OUT = DATA_DIR / "processed" / "ne_dnr_use.parquet"
COUNTY_AGG_OUT = DATA_DIR / "processed" / "ne_dnr_county_summary.parquet"


def _ne_county_to_fips() -> dict[str, str]:
    """Derive NE county-name → FIPS from the NASS parquet."""
    nass_path = DATA_DIR / "processed" / "nass_irrigated_acres.parquet"
    if not nass_path.exists():
        return {}
    nass = pd.read_parquet(nass_path)
    ne = nass[nass["state"] == "NE"][["county_name", "fips"]].drop_duplicates()
    return {n.upper(): f for n, f in zip(ne["county_name"], ne["fips"])}


def ingest_ne_wells() -> pd.DataFrame:
    """Parse NE DNR/DEE Groundwater_Wells_DWEE.csv (all 265k registered wells)."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    csvs = list(RAW_DIR.glob("*.csv"))
    if not csvs:
        log.warning("no NE DNR wells CSV in %s", RAW_DIR)
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for p in csvs:
        log.info("  reading %s (%.1f MB)", p.name, p.stat().st_size / 1e6)
        # Leading BOM on the first column — handle with utf-8-sig
        df = pd.read_csv(p, encoding="utf-8-sig", low_memory=False)
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)

    # Strip whitespace, coerce numerics
    for c in df.select_dtypes(include="object").columns:
        df[c] = df[c].astype(str).str.strip()
    for c in ("Latitude", "Longitude", "Acres", "PumpRate", "TotalDepth",
              "StaticWaterLevel", "PumpingWaterLevel", "PumpDepth"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["fips"] = df["CountyName"].str.upper().map(_ne_county_to_fips())
    df["is_active"] = df["Status"].str.contains("Active", case=False, na=False)
    df["is_irrigation"] = df["WellUseDescription"].str.contains(
        "Irrigation", case=False, na=False
    )

    WELLS_OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(WELLS_OUT, index=False)
    log.info(
        "Wrote %d NE wells (%d active, %d irrigation, %d FIPS-resolved) → %s",
        len(df), df["is_active"].sum(), df["is_irrigation"].sum(),
        df["fips"].notna().sum(), WELLS_OUT,
    )
    return df


def build_ne_county_summary() -> pd.DataFrame:
    """Aggregate NE wells to county-level features for the baseline.

    StaticWaterLevel is measured at well completion — so it's a *historical
    snapshot* per well, not current. For the baseline we use the median-across-wells
    for each county as a rough proxy, weighted toward wells completed more recently.
    """
    if not WELLS_OUT.exists():
        raise RuntimeError("run ingest_ne_wells first")
    df = pd.read_parquet(WELLS_OUT)

    # Filter to active irrigation wells in identified counties
    act = df[df["is_active"] & df["is_irrigation"] & df["fips"].notna()].copy()
    # Bias the median toward recent completions (past 25 years)
    act["CompletionDate"] = pd.to_datetime(act["CompletionDate"], errors="coerce", utc=True)
    recent = act[act["CompletionDate"] >= pd.Timestamp("1995-01-01", tz="UTC")]

    agg = act.groupby("fips").agg(
        n_active_irr_wells=("WellID", "count"),
        total_acres_served=("Acres", "sum"),
        median_total_depth_ft=("TotalDepth", "median"),
        median_pump_rate_gpm=("PumpRate", "median"),
    ).reset_index()

    recent_swl = recent.groupby("fips").agg(
        median_static_water_level_ft=("StaticWaterLevel", "median"),
        median_total_depth_recent_ft=("TotalDepth", "median"),
    ).reset_index()
    agg = agg.merge(recent_swl, on="fips", how="left")

    # Rough current saturated thickness: recent wells' TotalDepth − StaticWaterLevel.
    # Caveat: StaticWaterLevel is at well-completion time, not literally today —
    # so this trails actual water table by decades for slow-depleting counties.
    # Still far better than the 30 m HPA fallback for the 50+ NE HPA counties.
    agg["saturated_thickness_m"] = (
        (agg["median_total_depth_recent_ft"] - agg["median_static_water_level_ft"])
        * 0.3048
    ).clip(lower=0)

    COUNTY_AGG_OUT.parent.mkdir(parents=True, exist_ok=True)
    agg.to_parquet(COUNTY_AGG_OUT, index=False)
    log.info("Wrote NE county summary (%d counties) → %s", len(agg), COUNTY_AGG_OUT)
    return agg


def ingest_ne_nrd_use_reports() -> pd.DataFrame:
    """Optional — NRD annual use reports (separate manual drop)."""
    csvs = list(RAW_DIR.glob("nrd_use_*.csv"))
    if not csvs:
        log.info("no NRD annual use files yet (optional)")
        return pd.DataFrame()
    df = pd.concat([pd.read_csv(p) for p in csvs], ignore_index=True)
    df.to_parquet(USE_OUT, index=False)
    log.info("Wrote %d NE NRD use rows → %s", len(df), USE_OUT)
    return df


if __name__ == "__main__":
    ingest_ne_wells()
    build_ne_county_summary()
    ingest_ne_nrd_use_reports()
