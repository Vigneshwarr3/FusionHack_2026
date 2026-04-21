"""Download + zonal-mean the USGS McGuire HPA rasters.

Two authoritative published products feed this:

1. **Saturated thickness, 2009** — McGuire, Lund, Densmore 2012 (SIR 2012-5177),
   ScienceBase item `631405d0d34e36012efa33aa`, raster values in **feet**.
   The most recent USGS-published HPA-wide saturated-thickness raster that's
   publicly downloadable.

2. **Water-level change, predevelopment (~1950) to 2011** — same series
   (SIR 2012-5291), ScienceBase item `631405d0d34e36012efa33b6`, raster values
   in **feet** (negative = decline). Divided by 61 years to annualize.

Output
------
    data/processed/usgs_hpa_raster_thickness.parquet
        fips | thickness_m | annual_decline_m | n_pixels | source

Usage
-----
    poetry run python -m aquiferwatch.pipeline.usgs_hpa_rasters
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)

RAW_DIR = DATA_DIR / "raw" / "usgs_hpa"
PROCESSED = DATA_DIR / "processed"
OUTPUT = PROCESSED / "usgs_hpa_raster_thickness.parquet"

# Predevelopment year per McGuire — used to annualize water-level change.
PREDEV_YEAR = 1950
WLC_END_YEAR = 2011
WLC_YEARS = WLC_END_YEAR - PREDEV_YEAR  # 61

TIGER_URL = (
    "https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_county_500k.zip"
)
HPA_STATE_FIPS = {"08", "20", "31", "35", "40", "46", "48", "56"}

# Direct ScienceBase download URLs (resolved 2026-04-20 via the catalog JSON).
SOURCES = {
    "saturated_thickness_2009": {
        "url": "https://www.sciencebase.gov/catalog/file/get/631405d0d34e36012efa33aa?f=__disk__32%2F4b%2F28%2F324b28e38f480e2b553c2af632973c9d680922e9",
        "filename": "hp_satthk09.zip",
        "citation": "McGuire, V.L., Lund, K.D., Densmore, B.K., 2012, SIR 2012-5177",
    },
    "wlc_predev_to_2011": {
        "url": "https://www.sciencebase.gov/catalog/file/get/631405d0d34e36012efa33b6?f=__disk__48%2Ff5%2F36%2F48f536b517dbc758334d291381d2aeb1f674100d",
        "filename": "sir2012-5291_hp_wlcpd11g.zip",
        "citation": "McGuire, V.L., 2013, SIR 2012-5291",
    },
}


def _download(name: str, url: str, filename: str) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    dst = RAW_DIR / filename
    if dst.exists():
        log.info("  using cached %s (%.1f MB)", filename, dst.stat().st_size / 1024 / 1024)
        return dst
    log.info("  downloading %s …", name)
    r = requests.get(url, timeout=300, stream=True)
    r.raise_for_status()
    dst.write_bytes(r.content)
    log.info("  wrote %s (%.1f MB)", dst, dst.stat().st_size / 1024 / 1024)
    return dst


def _extract(zip_path: Path) -> Path:
    """Extract a ScienceBase zip to a sibling directory. Return extract dir."""
    out = zip_path.with_suffix("")
    if out.exists() and any(out.iterdir()):
        return out
    out.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(out)
    return out


def _find_raster(extract_dir: Path) -> Path:
    """Locate the raster inside an extracted McGuire zip.

    These releases contain ArcInfo Grid (`<name>/hdr.adf`) or GeoTIFFs. We
    prefer GeoTIFF if present, else the directory containing `hdr.adf`.
    """
    tifs = list(extract_dir.rglob("*.tif")) + list(extract_dir.rglob("*.TIF"))
    if tifs:
        return tifs[0]
    # ArcInfo Grid is identified by a directory containing hdr.adf
    for hdr in extract_dir.rglob("hdr.adf"):
        return hdr.parent
    # Some McGuire releases also contain `.asc` ASCII grids.
    ascs = list(extract_dir.rglob("*.asc"))
    if ascs:
        return ascs[0]
    # Fall back to a .e00 export (GDAL can read with "E00GRID" driver)
    e00s = list(extract_dir.rglob("*.e00"))
    if e00s:
        return e00s[0]
    raise FileNotFoundError(f"no raster found in {extract_dir}")


def _load_counties():
    """Load HPA-state TIGER county polygons from the cached TIGER shapefile."""
    import geopandas as gpd

    tiger_zip = DATA_DIR / "raw" / "tiger" / "cb_2022_us_county_500k.zip"
    if not tiger_zip.exists():
        log.info("  downloading TIGER counties …")
        r = requests.get(TIGER_URL, timeout=120)
        r.raise_for_status()
        tiger_zip.parent.mkdir(parents=True, exist_ok=True)
        tiger_zip.write_bytes(r.content)
    gdf = gpd.read_file(f"zip://{tiger_zip}")
    gdf = gdf[gdf["STATEFP"].isin(HPA_STATE_FIPS)].copy()
    gdf["fips"] = (gdf["STATEFP"].astype(str) + gdf["COUNTYFP"].astype(str)).str.zfill(5)
    return gdf[["fips", "geometry"]].reset_index(drop=True)


def _zonal_mean(raster_path: Path, counties) -> pd.DataFrame:
    """Compute per-county mean + pixel count, ignoring nodata + nonpositive values."""
    import rasterio
    from rasterio.mask import mask as rio_mask

    rows = []
    with rasterio.open(raster_path) as src:
        if src.crs is None:
            raise RuntimeError(f"{raster_path} has no CRS; cannot reproject counties")
        counties_proj = counties.to_crs(src.crs)
        nodata = src.nodata
        for fips, geom in zip(counties_proj["fips"], counties_proj.geometry):
            if geom is None or geom.is_empty:
                rows.append({"fips": fips, "raster_mean": np.nan, "n_pixels": 0})
                continue
            try:
                arr, _ = rio_mask(src, [geom], crop=True, filled=True)
            except (ValueError, rasterio.errors.RasterioIOError):
                rows.append({"fips": fips, "raster_mean": np.nan, "n_pixels": 0})
                continue
            data = arr[0].astype("float64")
            if nodata is not None:
                data = np.where(data == nodata, np.nan, data)
            # McGuire rasters store "no aquifer" as 0 or a sentinel — drop zeros
            # from saturated-thickness means (a county mostly off-aquifer should
            # report NaN, not a depressed mean).
            data = np.where(data <= 0, np.nan, data)
            valid = ~np.isnan(data)
            n = int(valid.sum())
            mean = float(np.nanmean(data)) if n else np.nan
            rows.append({"fips": fips, "raster_mean": mean, "n_pixels": n})
    return pd.DataFrame(rows)


def _zonal_mean_signed(raster_path: Path, counties) -> pd.DataFrame:
    """Same as `_zonal_mean` but keeps negative values (for water-level change)."""
    import rasterio
    from rasterio.mask import mask as rio_mask

    rows = []
    with rasterio.open(raster_path) as src:
        if src.crs is None:
            raise RuntimeError(f"{raster_path} has no CRS")
        counties_proj = counties.to_crs(src.crs)
        nodata = src.nodata
        for fips, geom in zip(counties_proj["fips"], counties_proj.geometry):
            if geom is None or geom.is_empty:
                rows.append({"fips": fips, "raster_mean": np.nan, "n_pixels": 0})
                continue
            try:
                arr, _ = rio_mask(src, [geom], crop=True, filled=True)
            except (ValueError, rasterio.errors.RasterioIOError):
                rows.append({"fips": fips, "raster_mean": np.nan, "n_pixels": 0})
                continue
            data = arr[0].astype("float64")
            if nodata is not None:
                data = np.where(data == nodata, np.nan, data)
            valid = ~np.isnan(data)
            n = int(valid.sum())
            mean = float(np.nanmean(data)) if n else np.nan
            rows.append({"fips": fips, "raster_mean": mean, "n_pixels": n})
    return pd.DataFrame(rows)


def build() -> pd.DataFrame:
    log.info("downloading USGS McGuire HPA rasters …")
    thick_zip = _download("saturated_thickness_2009", **{k: v for k, v in SOURCES["saturated_thickness_2009"].items() if k != "citation"})
    wlc_zip = _download("wlc_predev_to_2011", **{k: v for k, v in SOURCES["wlc_predev_to_2011"].items() if k != "citation"})

    log.info("extracting …")
    thick_dir = _extract(thick_zip)
    wlc_dir = _extract(wlc_zip)

    thick_raster = _find_raster(thick_dir)
    wlc_raster = _find_raster(wlc_dir)
    log.info("  thickness raster: %s", thick_raster)
    log.info("  water-level-change raster: %s", wlc_raster)

    counties = _load_counties()
    log.info("  %d HPA-state counties", len(counties))

    log.info("computing zonal-mean saturated thickness (feet, positive) …")
    thick = _zonal_mean(thick_raster, counties)
    thick["thickness_m"] = thick["raster_mean"] * 0.3048  # ft → m
    thick = thick[["fips", "thickness_m", "n_pixels"]].rename(columns={"n_pixels": "n_pixels_thick"})
    log.info("  %d / %d counties got a thickness value",
             thick["thickness_m"].notna().sum(), len(thick))

    log.info("computing zonal-mean water-level change (feet, predev→2011) …")
    wlc = _zonal_mean_signed(wlc_raster, counties)
    # Annualize: (total change in ft) × 0.3048 / 61 yrs → m/yr
    wlc["annual_decline_m"] = wlc["raster_mean"] * 0.3048 / WLC_YEARS
    wlc = wlc[["fips", "annual_decline_m", "n_pixels"]].rename(columns={"n_pixels": "n_pixels_wlc"})
    log.info("  %d / %d counties got a decline value",
             wlc["annual_decline_m"].notna().sum(), len(wlc))

    out = thick.merge(wlc, on="fips", how="outer")
    out["source"] = "usgs_mcguire_raster"
    out = out[["fips", "thickness_m", "annual_decline_m",
               "n_pixels_thick", "n_pixels_wlc", "source"]]

    PROCESSED.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT, index=False)
    log.info("wrote %s  (%d counties)", OUTPUT.relative_to(DATA_DIR.parent), len(out))
    return out


if __name__ == "__main__":
    build()
