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


def ingest_ne_wells() -> pd.DataFrame:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    csv = RAW_DIR / "registered_wells.csv"
    if not csv.exists():
        log.warning("no NE DNR wells CSV — manual download required")
        return pd.DataFrame()
    df = pd.read_csv(csv)
    df.to_parquet(WELLS_OUT, index=False)
    log.info("Wrote %d NE wells → %s", len(df), WELLS_OUT)
    return df


def ingest_ne_nrd_use_reports() -> pd.DataFrame:
    """Natural Resource District annual groundwater use summaries."""
    csvs = list(RAW_DIR.glob("nrd_use_*.csv"))
    if not csvs:
        log.warning("no NE NRD use reports — manual download required")
        return pd.DataFrame()
    df = pd.concat([pd.read_csv(p) for p in csvs], ignore_index=True)
    df.to_parquet(USE_OUT, index=False)
    log.info("Wrote %d NE NRD use rows → %s", len(df), USE_OUT)
    return df


if __name__ == "__main__":
    ingest_ne_wells()
    ingest_ne_nrd_use_reports()
