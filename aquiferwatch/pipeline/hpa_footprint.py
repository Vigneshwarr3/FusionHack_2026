"""USGS High Plains Aquifer footprint (USGS Data Series 543).

ScienceBase item: 6314061bd34e36012efa397b
Title: "Digital map of aquifer boundary for the High Plains aquifer in parts
of Colorado, Kansas, Nebraska, New Mexico, Oklahoma, South Dakota, Texas, and
Wyoming."

Outputs:
    data/raw/usgs/ds543.zip                (cached shapefile zip)
    data/processed/hpa_boundary.parquet    (GeoDataFrame in EPSG:4326)
"""

from __future__ import annotations

import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger, http_session

log = get_logger(__name__)

DS543_URL = (
    "https://www.sciencebase.gov/catalog/file/get/6314061bd34e36012efa397b"
    "?f=__disk__39%2F00%2Fb6%2F3900b621917a747319cd00a5ecb647623a500dae"
)
RAW = DATA_DIR / "raw" / "usgs" / "ds543.zip"
OUTPUT = DATA_DIR / "processed" / "hpa_boundary.parquet"


def download() -> Path:
    RAW.parent.mkdir(parents=True, exist_ok=True)
    if RAW.exists() and RAW.stat().st_size > 1000:
        return RAW
    session = http_session("AquiferWatch-HPA/0.1")
    log.info("  downloading HPA boundary (DS-543, ~1 MB)...")
    r = session.get(DS543_URL, timeout=120)
    r.raise_for_status()
    RAW.write_bytes(r.content)
    return RAW


def load() -> gpd.GeoDataFrame:
    """Extract the shapefile and load as GeoDataFrame in EPSG:4326."""
    zpath = download()
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(zpath) as zf:
            zf.extractall(tmp)
        # Find the .shp inside.
        shps = list(Path(tmp).rglob("*.shp"))
        if not shps:
            raise RuntimeError("no .shp in ds543.zip")
        gdf = gpd.read_file(shps[0])
    if gdf.crs is None:
        raise RuntimeError("HPA shapefile missing CRS")
    gdf = gdf.to_crs("EPSG:4326")
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(OUTPUT)
    log.info("Wrote HPA boundary (%d polygons) → %s", len(gdf), OUTPUT)
    log.info("  columns: %s", gdf.columns.tolist())
    log.info("  total area (deg²): %.3f", gdf.geometry.area.sum())
    return gdf


if __name__ == "__main__":
    load()
