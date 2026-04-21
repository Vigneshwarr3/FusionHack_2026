"""EIA industrial-sector electricity prices → per-state $/kWh.

Used to compute per-county pumping cost ($/AF) by combining with the
per-county pumping energy intensity (kwh_per_af_pumped, already in the
baseline from well-depth).

Sources
-------
- EIA API v2 (preferred): `api.eia.gov/v2/electricity/retail-sales/data/`
  Requires `EIA_API_KEY` env var (free register at
  https://www.eia.gov/opendata/register.php). We pull monthly industrial
  prices 2020-onwards and keep the most-recent 12-month average per state.

- Static fallback (used when no API key): published annual 2024 values
  from EIA's State Electricity Profiles
  https://www.eia.gov/electricity/state/
  These are stable enough for demo — industrial rates move ~5% YoY.

Output
------
    data/processed/eia_state_prices.parquet
        state | cents_per_kwh | year | source

Usage
-----
    poetry run python -m aquiferwatch.pipeline.eia_prices
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import requests

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)

PROCESSED = DATA_DIR / "processed"
OUTPUT = PROCESSED / "eia_state_prices.parquet"

HPA_STATES = ("NE", "KS", "CO", "TX", "OK", "NM", "SD", "WY")

# 2024 annual average industrial electricity price, cents/kWh, per state.
# Source: EIA State Electricity Profiles (2024 data release), Table 8.
# https://www.eia.gov/electricity/state/
# Updated 2026-04-20. Kept as last-resort fallback when no API key is set.
STATIC_2024_PRICES = {
    "NE": 7.97,
    "KS": 9.21,
    "CO": 9.06,
    "TX": 6.48,
    "OK": 6.61,
    "NM": 7.44,
    "SD": 7.27,
    "WY": 6.93,
}

API_URL = "https://api.eia.gov/v2/electricity/retail-sales/data/"


def _fetch_from_api(api_key: str) -> pd.DataFrame:
    """Pull monthly industrial prices for all 8 HPA states, 2020-2025."""
    params: list[tuple[str, str]] = [
        ("api_key", api_key),
        ("frequency", "monthly"),
        ("data[0]", "price"),
        ("facets[sectorid][]", "IND"),
        ("start", "2020-01"),
        ("end", "2025-12"),
        ("length", "5000"),
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "asc"),
    ]
    for s in HPA_STATES:
        params.append(("facets[stateid][]", s))
    r = requests.get(API_URL, params=params, timeout=60)
    r.raise_for_status()
    j = r.json()
    rows = j.get("response", {}).get("data", [])
    if not rows:
        raise RuntimeError("EIA returned no rows — check filters / key")
    df = pd.DataFrame(rows)
    # keep trailing-12-month mean per state
    df["period"] = pd.to_datetime(df["period"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    latest_year = df["period"].max().year
    recent = df[df["period"] >= f"{latest_year - 1}-01"]
    out = (
        recent.groupby("stateid")["price"]
        .mean()
        .reset_index()
        .rename(columns={"stateid": "state", "price": "cents_per_kwh"})
    )
    out["year"] = latest_year
    out["source"] = "eia_api_v2_monthly_industrial_trailing12"
    return out


def _static_fallback() -> pd.DataFrame:
    return pd.DataFrame(
        [{"state": s, "cents_per_kwh": v, "year": 2024,
          "source": "eia_state_profiles_2024_static"}
         for s, v in STATIC_2024_PRICES.items()]
    )


def build() -> pd.DataFrame:
    key = os.getenv("EIA_API_KEY", "").strip()
    if key:
        try:
            log.info("fetching EIA API (industrial monthly)…")
            out = _fetch_from_api(key)
            log.info("  got %d states from API", len(out))
        except Exception as e:
            log.warning("  EIA API failed (%s) — using static fallback", e)
            out = _static_fallback()
    else:
        log.info("EIA_API_KEY not set — using published 2024 static values")
        out = _static_fallback()

    PROCESSED.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT, index=False)
    log.info("wrote %s  (%d states, mean=%.2f ¢/kWh)",
             OUTPUT.relative_to(DATA_DIR.parent), len(out),
             out["cents_per_kwh"].mean())
    return out


if __name__ == "__main__":
    build()
