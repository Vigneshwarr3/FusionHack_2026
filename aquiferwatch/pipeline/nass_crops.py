"""USDA NASS QuickStats — county-level irrigated acres by crop.

Mirrors the pattern in the parent project
(`Agricultural_Data_Analysis/pipeline/quickstats_ingest.py`): one request per
(state × crop × year), JSON response, union to a single parquet.

Output: data/processed/nass_irrigated_acres.parquet
Columns: state_fips, county_fips, state, county, crop, year, value (acres)
"""

from __future__ import annotations

import argparse
import os

import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger, http_session

log = get_logger(__name__)

API_URL = "https://quickstats.nass.usda.gov/api/api_GET/"
OUTPUT = DATA_DIR / "processed" / "nass_irrigated_acres.parquet"

HPA_STATES = ("NE", "KS", "CO", "TX", "OK", "NM", "SD", "WY")
# USDA NASS commodity names map to our internal crop names.
CROPS = {
    "corn": "CORN",
    "soybeans": "SOYBEANS",
    "sorghum": "SORGHUM",
    "wheat": "WHEAT",
    "cotton": "COTTON",
    "alfalfa": "HAY",  # Alfalfa is reported under HAY with util_practice_desc=IRRIGATED
}


def _api_key() -> str:
    # Prefer env var; fall back to parent project's .env if present.
    key = os.environ.get("QUICKSTATS_API_KEY", "")
    if not key:
        parent_env = DATA_DIR.parent.parent / "Agricultural_Data_Analysis" / ".env"
        if parent_env.exists():
            for line in parent_env.read_text().splitlines():
                if line.startswith("QUICKSTATS_API_KEY="):
                    key = line.split("=", 1)[1].strip()
                    break
    if not key:
        raise RuntimeError("QUICKSTATS_API_KEY not set (env or parent .env)")
    return key


def fetch_one(state: str, crop_label: str, commodity: str, year: int) -> pd.DataFrame:
    """Census of Ag query for irrigated acres at county level.

    The annual SURVEY doesn't split by irrigation at county level — that split
    only exists in CENSUS (every 5 years, e.g. 2017 and 2022). For annual
    series we can optionally pair this with SURVEY totals and ratio-split.
    """
    session = http_session("AquiferWatch-NASS/0.1")
    params = {
        "key": _api_key(),
        "source_desc": "CENSUS",
        "sector_desc": "CROPS",
        "commodity_desc": commodity,
        "statisticcat_desc": "AREA HARVESTED",
        "prodn_practice_desc": "IRRIGATED",
        "unit_desc": "ACRES",
        "agg_level_desc": "COUNTY",
        "year": year,
        "state_alpha": state,
        "format": "JSON",
    }
    r = session.get(API_URL, params=params, timeout=60)
    # NASS returns 400 "no data matching query" for empty slices; treat as empty.
    if r.status_code == 400:
        return pd.DataFrame()
    r.raise_for_status()
    records = r.json().get("data", [])
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    df["crop"] = crop_label
    return df


def fetch_all(years: tuple[int, ...] = (2017, 2022)) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for state in HPA_STATES:
        for crop_label, commodity in CROPS.items():
            for year in years:
                try:
                    df = fetch_one(state, crop_label, commodity, year)
                except Exception as e:
                    log.warning("  [%s %s %d] failed: %s", state, commodity, year, e)
                    continue
                if not df.empty:
                    frames.append(df)
                    log.info("  [%s %s %d] %d rows", state, crop_label, year, len(df))
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)

    # Normalize + slim down to the columns we actually use.
    df["value"] = pd.to_numeric(df["Value"].str.replace(",", ""), errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    keep = [
        "state_fips_code", "county_code", "state_alpha", "state_name",
        "county_name", "crop", "year", "value",
    ]
    keep = [c for c in keep if c in df.columns]
    out = df[keep].rename(columns={
        "state_fips_code": "state_fips",
        "county_code": "county_fips_3",
        "state_alpha": "state",
        "state_name": "state_name",
        "county_name": "county_name",
    })
    # Full 5-digit FIPS
    if "state_fips" in out.columns and "county_fips_3" in out.columns:
        out["fips"] = out["state_fips"].astype(str).str.zfill(2) + out["county_fips_3"].astype(str).str.zfill(3)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT, index=False)
    log.info("Wrote %d NASS crop-acres rows → %s", len(out), OUTPUT)
    return out


def smoke() -> int:
    df = fetch_one("KS", "corn", "CORN", 2022)
    log.info("NASS KS corn 2022 smoke: %d rows", len(df))
    return len(df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    if args.smoke:
        smoke()
    if args.all:
        fetch_all()
