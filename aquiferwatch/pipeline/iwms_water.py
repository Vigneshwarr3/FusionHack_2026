"""USDA IWMS 2023 — water-applied-per-acre by crop and state.

Source: QuickStats API, CENSUS 2023 Irrigation and Water Management Survey,
`statisticcat_desc=WATER APPLIED` + `unit_desc=ACRE FEET / ACRE`.

Output: data/processed/iwms_water_per_acre.parquet
Columns: state, crop, year, acre_feet_per_acre
"""

from __future__ import annotations

import argparse

import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger, http_session
from aquiferwatch.pipeline.nass_crops import CROPS, HPA_STATES, _api_key

log = get_logger(__name__)

API_URL = "https://quickstats.nass.usda.gov/api/api_GET/"
OUTPUT = DATA_DIR / "processed" / "iwms_water_per_acre.parquet"


def fetch_one(state: str, crop_label: str, commodity: str, year: int) -> pd.DataFrame:
    session = http_session("AquiferWatch-IWMS/0.1")
    params = {
        "key": _api_key(),
        "source_desc": "CENSUS",
        "sector_desc": "CROPS",
        "commodity_desc": commodity,
        "statisticcat_desc": "WATER APPLIED",
        "unit_desc": "ACRE FEET / ACRE",
        "agg_level_desc": "STATE",
        "year": year,
        "state_alpha": state,
        "format": "JSON",
    }
    r = session.get(API_URL, params=params, timeout=60)
    if r.status_code == 400:
        return pd.DataFrame()
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    df["crop"] = crop_label
    return df


def fetch_all(year: int = 2023) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for state in HPA_STATES:
        for crop_label, commodity in CROPS.items():
            try:
                df = fetch_one(state, crop_label, commodity, year)
            except Exception as e:
                log.warning("  [%s %s %d] failed: %s", state, commodity, year, e)
                continue
            if not df.empty:
                frames.append(df)
                log.info("  [%s %s %d] %d rows", state, crop_label, year, len(df))
    if not frames:
        log.warning("no IWMS data returned")
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df["acre_feet_per_acre"] = pd.to_numeric(
        df["Value"].astype(str).str.replace(",", ""), errors="coerce"
    )
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    out = df[["state_alpha", "state_name", "crop", "year", "acre_feet_per_acre"]].rename(
        columns={"state_alpha": "state"}
    )
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT, index=False)
    log.info("Wrote %d IWMS rows → %s", len(out), OUTPUT)
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2023)
    args = parser.parse_args()
    fetch_all(args.year)
