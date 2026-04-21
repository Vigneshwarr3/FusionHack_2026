"""NOAA nClimDiv county precipitation.

Pulls the NCEI climdiv per-county monthly precipitation file and derives:
    - `precip_normal_mm_yr`    — 1991–2020 annual normal
    - `precip_recent_mm_yr`    — mean of 2019-2023 annual totals
    - `precip_anomaly_pct`     — (recent − normal) / normal × 100

One flat text file covers every U.S. county 1895→present. No API key.
NOAA units are hundredths of inches; we convert to mm.

File format (fixed-width per NCEI docs):
    cols 1-2   state FIPS
    cols 3-5   county FIPS
    cols 6-7   element code (01 = precipitation)
    cols 8-11  year
    cols 12-18 Jan value  ... cols 83-89 Dec value
    Values are in hundredths of inches; missing = -9999.

Source
------
https://www.ncei.noaa.gov/pub/data/cirs/climdiv/

Output
------
    data/processed/noaa_county_precip.parquet
        fips | precip_normal_mm_yr | precip_recent_mm_yr | precip_anomaly_pct

Usage
-----
    poetry run python -m aquiferwatch.pipeline.noaa_climdiv
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import requests

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)

PROCESSED = DATA_DIR / "processed"
RAW_DIR = DATA_DIR / "raw" / "noaa_climdiv"
OUTPUT = PROCESSED / "noaa_county_precip.parquet"

# Directory listing — we pick the newest `climdiv-pcpncy-*` file we find.
INDEX_URL = "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/"

# NCEI climdiv uses NCDC state codes, NOT Census FIPS. Mapping for the
# 8 HPA states and back to FIPS for the join with our baseline:
NCDC_TO_FIPS = {
    "05": "08",  # Colorado
    "14": "20",  # Kansas
    "25": "31",  # Nebraska
    "29": "35",  # New Mexico
    "34": "40",  # Oklahoma
    "39": "46",  # South Dakota
    "41": "48",  # Texas
    "48": "56",  # Wyoming
}
HPA_NCDC_CODES = set(NCDC_TO_FIPS.keys())
NORMAL_START = 1991
NORMAL_END = 2020
RECENT_START = 2019
RECENT_END = 2023


def _latest_file_url() -> str:
    """Find the latest climdiv-pcpncy-* filename in the NCEI directory listing."""
    r = requests.get(INDEX_URL, timeout=60)
    r.raise_for_status()
    # Apache-style directory listing HTML — grep for the filename pattern
    import re
    # Match the filename stopping at <, space, or quote — NCEI listings wrap
    # filenames in anchor tags so we must not capture the closing HTML.
    matches = re.findall(r'(climdiv-pcpncy-v[\d.]+-\d{8})', r.text)
    if not matches:
        raise RuntimeError("couldn't find climdiv-pcpncy in NCEI listing")
    # Most recent — the listing tends to be alphabetical so the latest date wins
    latest = sorted(set(matches))[-1]
    return INDEX_URL + latest


def _download(url: str) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    dst = RAW_DIR / url.rsplit("/", 1)[-1]
    if dst.exists():
        log.info("  using cached %s (%.1f MB)", dst.name, dst.stat().st_size / 1024 / 1024)
        return dst
    log.info("  downloading %s", url)
    r = requests.get(url, timeout=300, stream=True)
    r.raise_for_status()
    dst.write_bytes(r.content)
    log.info("  wrote %s (%.1f MB)", dst, dst.stat().st_size / 1024 / 1024)
    return dst


def _parse_climdiv(path: Path) -> pd.DataFrame:
    """Parse the climdiv fixed-width file into tidy (fips, year, annual_mm)."""
    # The file actually uses whitespace separators for the 12 month values
    # after an 11-char header (SSCCCEEYYYY). Some newer versions vary — safest:
    # read with fixed widths for the 11-char header then whitespace for the rest.
    rows: list[dict] = []
    with open(path, "r") as f:
        for line in f:
            if len(line) < 90:
                continue
            header = line[:11]
            rest = line[11:].split()
            if len(rest) < 12:
                continue
            ncdc_state = header[0:2]
            county_code = header[2:5]
            element = header[5:7]
            year_s = header[7:11]
            if element != "01":  # 01 = precipitation
                continue
            try:
                year = int(year_s)
            except ValueError:
                continue
            if ncdc_state not in HPA_NCDC_CODES:
                continue
            # Translate NCDC state code -> Census FIPS for the baseline join.
            fips = NCDC_TO_FIPS[ncdc_state] + county_code
            try:
                months = [float(v) for v in rest[:12]]
            except ValueError:
                continue
            # Missing = -9.99 (inches, not hundredths for this file format).
            # NCEI pcpncy actually reports inches to 2 decimal places, missing = -9.99.
            valid = [m for m in months if m > -9.0]
            if len(valid) < 12:
                continue
            annual_in = sum(months)
            annual_mm = annual_in * 25.4
            rows.append({"fips": fips, "year": year, "annual_mm": annual_mm})
    return pd.DataFrame(rows)


def build() -> pd.DataFrame:
    url = _latest_file_url()
    path = _download(url)
    panel = _parse_climdiv(path)
    log.info("  parsed %d (fips, year) rows across %d counties, years %d–%d",
             len(panel), panel["fips"].nunique(),
             panel["year"].min(), panel["year"].max())

    # Normal (1991-2020)
    normal = (
        panel[(panel["year"] >= NORMAL_START) & (panel["year"] <= NORMAL_END)]
        .groupby("fips")["annual_mm"].mean().rename("precip_normal_mm_yr")
    )
    # Recent (2019-2023)
    recent = (
        panel[(panel["year"] >= RECENT_START) & (panel["year"] <= RECENT_END)]
        .groupby("fips")["annual_mm"].mean().rename("precip_recent_mm_yr")
    )
    out = pd.concat([normal, recent], axis=1).reset_index()
    out["precip_anomaly_pct"] = np.where(
        out["precip_normal_mm_yr"] > 0,
        (out["precip_recent_mm_yr"] - out["precip_normal_mm_yr"])
        / out["precip_normal_mm_yr"] * 100,
        np.nan,
    )
    PROCESSED.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT, index=False)
    log.info("wrote %s  (%d counties, normal median=%.0f mm/yr, recent median=%.0f mm/yr)",
             OUTPUT.relative_to(DATA_DIR.parent), len(out),
             out["precip_normal_mm_yr"].median(),
             out["precip_recent_mm_yr"].median())
    return out


if __name__ == "__main__":
    build()
