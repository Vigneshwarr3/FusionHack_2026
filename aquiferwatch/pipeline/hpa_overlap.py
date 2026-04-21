"""Compute per-county High Plains Aquifer overlap fraction.

Many "HPA state" counties aren't actually over the aquifer (e.g., east Texas,
west Colorado). Without this filter the frontend ends up painting a huge
off-aquifer halo with the 30m fallback, which visually swamps the real story.

Inputs
------
- `data/processed/hpa_boundary.parquet` (USGS HPA aquifer boundary polygons)
- TIGER `cb_2022_us_county_500k` (downloaded on demand, cached)

Output
------
    data/processed/hpa_county_overlap.parquet
        fips | overlap_pct | overlap_area_km2 | county_area_km2

Usage
-----
    poetry run python -m aquiferwatch.pipeline.hpa_overlap
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)

PROCESSED = DATA_DIR / "processed"
OUTPUT = PROCESSED / "hpa_county_overlap.parquet"

TIGER_URL = (
    "https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_county_500k.zip"
)
TIGER_ZIP = DATA_DIR / "raw" / "tiger" / "cb_2022_us_county_500k.zip"
HPA_STATE_FIPS = {"08", "20", "31", "35", "40", "46", "48", "56"}


def build() -> pd.DataFrame:
    import geopandas as gpd
    from shapely.ops import unary_union

    hp = gpd.read_parquet(PROCESSED / "hpa_boundary.parquet")
    hp_present = hp[hp["AQUIFER"] == "High Plains aquifer"]
    log.info("HPA present polygons: %d", len(hp_present))
    hp_union = unary_union(hp_present.geometry.values)

    if not TIGER_ZIP.exists():
        log.info("downloading TIGER counties …")
        TIGER_ZIP.parent.mkdir(parents=True, exist_ok=True)
        r = requests.get(TIGER_URL, timeout=120)
        r.raise_for_status()
        TIGER_ZIP.write_bytes(r.content)

    counties = gpd.read_file(f"zip://{TIGER_ZIP}")
    counties = counties[counties["STATEFP"].isin(HPA_STATE_FIPS)].copy()
    counties["fips"] = (
        counties["STATEFP"].astype(str) + counties["COUNTYFP"].astype(str)
    ).str.zfill(5)
    counties = counties.to_crs(hp.crs)
    log.info("HPA-state counties: %d", len(counties))

    # Reproject to equal-area for accurate areas.
    # EPSG:5070 is Albers Equal Area Conic for CONUS (area in m²).
    counties_proj = counties[["fips", "geometry"]].to_crs("EPSG:5070")
    hp_proj = gpd.GeoSeries([hp_union], crs=hp.crs).to_crs("EPSG:5070").iloc[0]

    rows = []
    for fips, geom in zip(counties_proj["fips"], counties_proj.geometry):
        county_area_m2 = float(geom.area)
        if county_area_m2 <= 0:
            rows.append({
                "fips": fips, "overlap_pct": 0.0,
                "overlap_area_km2": 0.0, "county_area_km2": 0.0,
            })
            continue
        inter = geom.intersection(hp_proj)
        overlap_area_m2 = float(inter.area) if not inter.is_empty else 0.0
        rows.append({
            "fips": fips,
            "overlap_pct": overlap_area_m2 / county_area_m2,
            "overlap_area_km2": overlap_area_m2 / 1e6,
            "county_area_km2": county_area_m2 / 1e6,
        })

    out = pd.DataFrame(rows)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT, index=False)
    log.info("wrote %s  (%d counties)", OUTPUT.relative_to(DATA_DIR.parent), len(out))
    over = out[out["overlap_pct"] > 0]
    log.info(
        "  %d counties with any HPA overlap; %d with >=10%%; %d with >=50%%",
        len(over),
        (out["overlap_pct"] >= 0.10).sum(),
        (out["overlap_pct"] >= 0.50).sum(),
    )
    return out


if __name__ == "__main__":
    build()
