"""Kansas Geological Survey WIMAS ingest.

Kansas is our pilot state for rigor (spec §7 risk 1): best-instrumented, metered
extractions by well. Use it as ground truth for Texas imputation validation.

**Manual download required** — WIMAS's interactive query interface at
`hercules.kgs.ku.edu` is not programmatically scriptable (form-based CFM,
currently intermittently unavailable from our network). Download steps:

  1. Visit https://www.kgs.ku.edu/Magellan/WaterWell/
  2. Under "Water Rights" → "Water Use", run the query for HPA counties.
  3. Export CSV to `data/raw/kgs_wimas/wimas_pumpage_<YYYY>.csv`.
  4. Run `python -m aquiferwatch.pipeline.kgs_wimas` to parse and write parquet.

Alternative: Kansas Department of Agriculture DWR WRIS (Water Rights Info
System) accepts annual batch exports by GMD (Groundwater Management District).

Once a CSV is dropped in `data/raw/kgs_wimas/`, this module parses it into
`data/processed/kgs_wimas_pumpage.parquet` with columns
(well_id, year, county_fips, acre_feet, use_type, gmd).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)

RAW_DIR = DATA_DIR / "raw" / "kgs_wimas"
OUTPUT = DATA_DIR / "processed" / "kgs_wimas_pumpage.parquet"


def ingest_kgs_pumpage() -> pd.DataFrame:
    """Parse any CSVs present in data/raw/kgs_wimas/ into a single parquet."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    csvs = list(RAW_DIR.glob("*.csv"))
    if not csvs:
        log.warning(
            "no CSVs in %s — manual download required, see module docstring",
            RAW_DIR,
        )
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for p in csvs:
        log.info("  parsing %s", p.name)
        df = pd.read_csv(p)
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT, index=False)
    log.info("Wrote %d WIMAS rows → %s", len(df), OUTPUT)
    return df


if __name__ == "__main__":
    ingest_kgs_pumpage()
