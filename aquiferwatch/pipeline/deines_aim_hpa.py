"""Deines et al. 2019 AIM-HPA annual irrigation pixels → per-county acres.

Dataset: Annual Irrigation Maps - High Plains Aquifer (AIM-HPA), 30 m
binary irrigated/non-irrigated rasters 1984-2017, EPSG:5070 CONUS Albers.
Reference: Deines, J.M., Kendall, A.D., Crowley, M.A., Rapp, J., Cardille,
J.A., Hyndman, D.W., 2019, "Mapping three decades of annual irrigation
across the US High Plains Aquifer using Landsat and Google Earth Engine,"
Remote Sensing of Environment 233: 111400.
Distribution: HydroShare resource a371fd69d41b4232806d81e17fe4efcb (CC-BY).

What this produces
------------------
    data/processed/deines_annual_irrigated_acres.parquet
        fips | year | irrigated_acres_deines | pixels_irrigated

Only HPA-footprint counties (hpa_county_overlap.parquet overlap_pct > 0)
are scanned — off-aquifer counties have no irrigation in this dataset
and zonal-sum would just be zero.

Each pixel is 30 × 30 m = 900 m² = 0.222395 acres.

Usage
-----
    poetry run python -m aquiferwatch.pipeline.deines_aim_hpa
    poetry run python -m aquiferwatch.pipeline.deines_aim_hpa --years 2010 2011  # subset
"""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)

PROCESSED = DATA_DIR / "processed"
OUTPUT = PROCESSED / "deines_annual_irrigated_acres.parquet"
# The HydroShare bag nests the contents/ folder two levels deep.
RAW_ROOT = DATA_DIR / "raw" / "deines_aim_hpa"

# 30 × 30 m pixel → acres
PIXEL_ACRES = (30 * 30) / 4046.8564224
# AIM-HPA binary values: 0 = non-irrigated, 1 = irrigated, 255 = nodata.
IRRIGATED_VALUE = 1
NODATA_VALUE = 255

TIGER_ZIP = DATA_DIR / "raw" / "tiger" / "cb_2022_us_county_500k.zip"


def _find_rasters() -> dict[int, Path]:
    """Walk the HydroShare extract and collect year → tif path."""
    rasters: dict[int, Path] = {}
    for tif in RAW_ROOT.rglob("*.tif"):
        m = re.match(r"(\d{4})_AIM-HPA", tif.name)
        if m:
            rasters[int(m.group(1))] = tif
    if not rasters:
        raise FileNotFoundError(f"No AIM-HPA rasters found under {RAW_ROOT}")
    return dict(sorted(rasters.items()))


def _load_counties():
    """HPA-footprint counties only (overlap > 0) — off-aquifer = 0 irrigation."""
    import geopandas as gpd

    overlap = pd.read_parquet(PROCESSED / "hpa_county_overlap.parquet")
    overlap["fips"] = overlap["fips"].astype(str).str.zfill(5)
    on_hpa = set(overlap.loc[overlap["overlap_pct"] > 0, "fips"])

    counties = gpd.read_file(f"zip://{TIGER_ZIP}")
    counties["fips"] = (counties["STATEFP"].astype(str)
                        + counties["COUNTYFP"].astype(str)).str.zfill(5)
    counties = counties[counties["fips"].isin(on_hpa)].copy()
    return counties[["fips", "geometry"]].reset_index(drop=True)


def _zonal_irrigated(raster_path: Path, counties) -> pd.DataFrame:
    """Per-county count of `irrigated` pixels in one annual raster."""
    import rasterio
    from rasterio.mask import mask as rio_mask

    rows: list[dict] = []
    with rasterio.open(raster_path) as src:
        counties_proj = counties.to_crs(src.crs)
        for fips, geom in zip(counties_proj["fips"], counties_proj.geometry):
            if geom is None or geom.is_empty:
                rows.append({"fips": fips, "pixels_irrigated": 0})
                continue
            try:
                arr, _ = rio_mask(src, [geom], crop=True, filled=True,
                                  nodata=NODATA_VALUE)
            except (ValueError, rasterio.errors.RasterioIOError):
                rows.append({"fips": fips, "pixels_irrigated": 0})
                continue
            data = arr[0]
            count = int(np.sum(data == IRRIGATED_VALUE))
            rows.append({"fips": fips, "pixels_irrigated": count})
    return pd.DataFrame(rows)


def build(year_filter: list[int] | None = None) -> pd.DataFrame:
    rasters = _find_rasters()
    if year_filter:
        rasters = {y: p for y, p in rasters.items() if y in year_filter}
    log.info("Found %d AIM-HPA annual rasters: %s",
             len(rasters), sorted(rasters.keys()))

    counties = _load_counties()
    log.info("Zonal-sum target: %d HPA-footprint counties", len(counties))

    all_years: list[pd.DataFrame] = []
    t0 = time.time()
    for i, (year, path) in enumerate(rasters.items()):
        year_t0 = time.time()
        df = _zonal_irrigated(path, counties)
        df["year"] = year
        df["irrigated_acres_deines"] = df["pixels_irrigated"] * PIXEL_ACRES
        all_years.append(df)
        elapsed_year = time.time() - year_t0
        log.info("  %d: %d counties, %dK pixels total, median=%d ac, %.1fs",
                 year,
                 int((df["pixels_irrigated"] > 0).sum()),
                 int(df["pixels_irrigated"].sum() / 1000),
                 int(df["irrigated_acres_deines"].median()),
                 elapsed_year)

    out = pd.concat(all_years, ignore_index=True)
    out = out[["fips", "year", "irrigated_acres_deines", "pixels_irrigated"]]
    PROCESSED.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT, index=False)
    total_s = time.time() - t0
    log.info("wrote %s  (%d rows across %d counties × %d years, %.0fs total)",
             OUTPUT.relative_to(DATA_DIR.parent), len(out),
             out["fips"].nunique(), out["year"].nunique(), total_s)
    return out


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--years", type=int, nargs="+",
                   help="Restrict to these years (e.g. --years 2010 2011)")
    args = p.parse_args()
    build(year_filter=args.years)
