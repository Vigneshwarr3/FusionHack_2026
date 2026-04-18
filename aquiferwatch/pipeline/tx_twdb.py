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
WELLS_OUT = DATA_DIR / "processed" / "twdb_wells.parquet"
GCD_OUT = DATA_DIR / "processed" / "twdb_gcd.parquet"


def ingest_tx_wells() -> pd.DataFrame:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    csvs = list(RAW_DIR.glob("wells_*.csv"))
    if not csvs:
        log.warning("no TWDB well CSVs — manual download required")
        return pd.DataFrame()
    df = pd.concat([pd.read_csv(p) for p in csvs], ignore_index=True)
    df.to_parquet(WELLS_OUT, index=False)
    log.info("Wrote %d TWDB wells → %s", len(df), WELLS_OUT)
    return df


def ingest_tx_gcd_boundaries() -> None:
    shp_dir = RAW_DIR / "gcd"
    if not shp_dir.exists():
        log.warning("no TWDB GCD shapefile — manual download required")
        return
    import geopandas as gpd

    shps = list(shp_dir.rglob("*.shp"))
    if not shps:
        log.warning("no .shp under %s", shp_dir)
        return
    gdf = gpd.read_file(shps[0]).to_crs("EPSG:4326")
    gdf.to_parquet(GCD_OUT)
    log.info("Wrote %d TWDB GCDs → %s", len(gdf), GCD_OUT)


if __name__ == "__main__":
    ingest_tx_wells()
    ingest_tx_gcd_boundaries()
