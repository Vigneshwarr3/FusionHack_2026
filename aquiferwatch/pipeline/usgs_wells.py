"""USGS High Plains Aquifer monitoring-well ingest.

Two pulls:
  1. Site inventory per HPA state via `nwis/site/` (RDB format)
  2. Annual groundwater levels per well via `nwis/gwlevels/` (RDB format)

Filters to wells with the High Plains aquifer code (N100HGHPLN), which is
USGS's national-aquifer classification and matches the polygons in the
HPA ScienceBase shapefile.

Outputs:
  data/raw/usgs/sites_{state}.rdb            (cached RDB per state)
  data/processed/usgs_wells.parquet          (combined wells table)
  data/processed/usgs_gwlevels.parquet       (combined water-level history)
"""

from __future__ import annotations

import argparse
import io

import pandas as pd

from aquiferwatch.config import DATA_DIR, settings
from aquiferwatch.pipeline.common import get_logger, http_session

log = get_logger(__name__)

# 8 HPA states (spec §3)
HPA_STATES: tuple[str, ...] = ("NE", "KS", "CO", "TX", "OK", "NM", "SD", "WY")
# National aquifer code for the High Plains aquifer.
HPA_AQFR_CODE = "N100HGHPLN"

SITE_URL = "https://waterservices.usgs.gov/nwis/site/"
# Legacy gwlevels was decommissioned Fall 2025. Use the new OGC API.
OGC_BASE = "https://api.waterdata.usgs.gov/ogcapi/v0"
FIELD_MEAS_URL = f"{OGC_BASE}/collections/field-measurements/items"
# Parameter code 72019 = Depth to water level, feet below land surface.
DEPTH_TO_WATER_PARAM = "72019"

RAW_DIR = DATA_DIR / "raw" / "usgs"
PROCESSED_DIR = DATA_DIR / "processed"
WELLS_PARQUET = PROCESSED_DIR / "usgs_wells.parquet"
GWLEVELS_PARQUET = PROCESSED_DIR / "usgs_gwlevels.parquet"


def _read_rdb(text: str) -> pd.DataFrame:
    """Parse USGS RDB (tab-separated with `#`-comment header + one format row)."""
    lines = [ln for ln in text.splitlines() if not ln.startswith("#")]
    if len(lines) < 3:
        return pd.DataFrame()
    header = lines[0].split("\t")
    # Line 1 is the format row (e.g. 15s, 5s, ...) — skip it.
    data_rows = [ln.split("\t") for ln in lines[2:]]
    return pd.DataFrame(data_rows, columns=header)


def fetch_sites(state: str) -> pd.DataFrame:
    """Site inventory for one HPA state, restricted to HPA aquifer wells."""
    cache = RAW_DIR / f"sites_{state}.rdb"
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if cache.exists() and cache.stat().st_size > 0:
        log.info("  [%s] using cached sites", state)
        text = cache.read_text(encoding="utf-8")
    else:
        session = http_session(settings.usgs_user_agent)
        params = {
            "format": "rdb",
            "stateCd": state.lower(),
            "siteType": "GW",
            "aquiferCd": HPA_AQFR_CODE,
            "siteStatus": "all",
            "hasDataTypeCd": "gw",
            "siteOutput": "expanded",
        }
        log.info("  [%s] fetching sites...", state)
        r = session.get(SITE_URL, params=params, timeout=60)
        r.raise_for_status()
        text = r.text
        cache.write_text(text, encoding="utf-8")

    df = _read_rdb(text)
    if df.empty:
        return df
    df["state_abbr"] = state
    # Coerce numeric lat/lon/depth
    for col in ("dec_lat_va", "dec_long_va", "well_depth_va", "alt_va"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def fetch_all_sites() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for state in HPA_STATES:
        try:
            frames.append(fetch_sites(state))
        except Exception as e:
            log.warning("  [%s] sites fetch failed: %s", state, e)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["dec_lat_va", "dec_long_va"])
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(WELLS_PARQUET, index=False)
    log.info("Wrote %d HPA wells → %s", len(df), WELLS_PARQUET)
    return df


def fetch_gwlevels(
    site_numbers: list[str],
    agency: str = "USGS",
    max_sites: int | None = None,
    datetime_range: str = "2000-01-01/..",
    sleep_per_req: float = 0.2,
) -> pd.DataFrame:
    """Pull groundwater-level field measurements via the OGC API.

    The OGC API uses `monitoring_location_id` formatted as "{agency}-{site_no}".
    One request per site (bulk OR-filters aren't supported). `sleep_per_req`
    throttles to stay under the per-IP rate limit — without this, sustained
    pulls trip 429s after a few hundred requests.
    """
    import time

    session = http_session(settings.usgs_user_agent)
    if max_sites is not None:
        site_numbers = site_numbers[:max_sites]

    frames: list[pd.DataFrame] = []
    rate_limit_hits = 0
    for i, sn in enumerate(site_numbers):
        if i and i % 500 == 0:
            log.info("  gwlevels %d/%d (%d rows, %d rate-limit hits so far)",
                     i, len(site_numbers), sum(len(f) for f in frames), rate_limit_hits)
        mlid = f"{agency}-{sn}"
        offset = 0
        while True:
            params = {
                "parameter_code": DEPTH_TO_WATER_PARAM,
                "monitoring_location_id": mlid,
                "limit": 10000,
                "offset": offset,
                "f": "json",
            }
            if datetime_range:
                params["datetime"] = datetime_range
            try:
                r = session.get(FIELD_MEAS_URL, params=params, timeout=60)
                if r.status_code == 429:
                    rate_limit_hits += 1
                    time.sleep(5.0)
                    continue
                r.raise_for_status()
            except Exception as e:
                log.debug("  [%s] failed: %s", mlid, e)
                break
            j = r.json()
            feats = j.get("features", [])
            if not feats:
                break
            frames.append(pd.DataFrame(f["properties"] for f in feats))
            if len(feats) < 10000:
                break
            offset += len(feats)
        time.sleep(sleep_per_req)

    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)

    # Normalize: strip the USGS- prefix, parse time + numeric value.
    df["site_no"] = df["monitoring_location_id"].str.replace(r"^[A-Z]+-", "", regex=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)
    df = df.dropna(subset=["value", "time"])
    keep = ["site_no", "time", "value", "unit_of_measure", "observing_procedure_code",
            "observing_procedure", "approval_status", "parameter_code"]
    return df[[c for c in keep if c in df.columns]]


def fetch_gwlevels_full(
    wells_df: pd.DataFrame | None = None, max_sites: int | None = None
) -> pd.DataFrame:
    """End-to-end: all HPA wells → historical water levels → parquet.

    With 48k+ wells and one request per site, a full pull is O(hours). Pass
    `max_sites` to cap during development; omit for production.
    """
    if wells_df is None:
        wells_df = pd.read_parquet(WELLS_PARQUET) if WELLS_PARQUET.exists() else fetch_all_sites()
    if wells_df.empty:
        log.warning("no wells cached — run fetch_all_sites first")
        return pd.DataFrame()
    sites = wells_df["site_no"].astype(str).unique().tolist()
    log.info("pulling gwlevels for %d wells%s",
             len(sites), f" (capped at {max_sites})" if max_sites else "")
    df = fetch_gwlevels(sites, max_sites=max_sites)
    if not df.empty:
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        df.to_parquet(GWLEVELS_PARQUET, index=False)
        log.info("Wrote %d water-level rows → %s", len(df), GWLEVELS_PARQUET)
    return df


def fetch_hpa_wells_smoke() -> int:
    """Minimal smoke test — pull Kansas only."""
    df = fetch_sites("KS")
    log.info("KS smoke: %d sites", len(df))
    return len(df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--sites", action="store_true", help="all HPA state sites")
    parser.add_argument("--gwlevels", action="store_true", help="water-level history")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    if args.smoke:
        fetch_hpa_wells_smoke()
    if args.sites or args.all:
        fetch_all_sites()
    if args.gwlevels or args.all:
        fetch_gwlevels_full()
