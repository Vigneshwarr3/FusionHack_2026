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
# 2024: added SIR 2023-5143 (McGuire & Strauch 2024) as the preferred decline
# source (69-year horizon, more recent data). Fallback to SIR 2012-5291 if
# the 2019 grid fails.
PREDEV_YEAR = 1950
WLC_END_YEAR_PRIMARY = 2019  # SIR 2023-5143 (preferred)
WLC_END_YEAR_LEGACY = 2011   # SIR 2012-5291 (kept for audit / fallback)
WLC_YEARS_PRIMARY = WLC_END_YEAR_PRIMARY - PREDEV_YEAR  # 69
WLC_YEARS_LEGACY = WLC_END_YEAR_LEGACY - PREDEV_YEAR    # 61

TIGER_URL = (
    "https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_county_500k.zip"
)
HPA_STATE_FIPS = {"08", "20", "31", "35", "40", "46", "48", "56"}

# Direct ScienceBase download URLs (resolved 2026-04-20 via the catalog JSON).
SOURCES = {
    # SIR 2012-5177 — still the most recent publicly-published saturated
    # thickness raster. SIR 2023-5143 derives 2019 thickness by adding
    # biennial water-level-change rasters to this 2009 baseline.
    "saturated_thickness_2009": {
        "url": "https://www.sciencebase.gov/catalog/file/get/631405d0d34e36012efa33aa?f=__disk__32%2F4b%2F28%2F324b28e38f480e2b553c2af632973c9d680922e9",
        "filename": "hp_satthk09.zip",
        "citation": "McGuire, V.L., Lund, K.D., Densmore, B.K., 2012, SIR 2012-5177",
    },
    # SIR 2023-5143 predev→2019 (preferred decline source, 69-year horizon).
    "wlc_predev_to_2019": {
        "url": "https://www.sciencebase.gov/catalog/file/get/6499c01dd34ef77fcb02b220?f=__disk__b9%2F8d%2Fd7%2Fb98dd76700f056f7496331fd35d1560316bf5155",
        "filename": "hp_wlcpd19t.zip",
        "citation": "McGuire & Strauch, 2024, SIR 2023-5143 (predev→2019)",
    },
    # SIR 2023-5143 2017→2019 (recent-biennium decline, for validation).
    "wlc_2017_to_2019": {
        "url": "https://www.sciencebase.gov/catalog/file/get/6499c112d34ef77fcb02b236?f=__disk__f2%2F99%2F0e%2Ff2990ec89285a258f8477b20ccbb6f4a73a4388a",
        "filename": "hp_wlc1719t.zip",
        "citation": "McGuire & Strauch, 2024, SIR 2023-5143 (2017–2019)",
    },
    # SIR 2012-5291 predev→2011 (legacy — keep as secondary for audit).
    "wlc_predev_to_2011": {
        "url": "https://www.sciencebase.gov/catalog/file/get/631405d0d34e36012efa33b6?f=__disk__48%2Ff5%2F36%2F48f536b517dbc758334d291381d2aeb1f674100d",
        "filename": "sir2012-5291_hp_wlcpd11g.zip",
        "citation": "McGuire, V.L., 2013, SIR 2012-5291 (legacy)",
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
    thick_zip = _download(
        "saturated_thickness_2009",
        url=SOURCES["saturated_thickness_2009"]["url"],
        filename=SOURCES["saturated_thickness_2009"]["filename"],
    )
    wlc_19_zip = _download(
        "wlc_predev_to_2019",
        url=SOURCES["wlc_predev_to_2019"]["url"],
        filename=SOURCES["wlc_predev_to_2019"]["filename"],
    )
    wlc_1719_zip = _download(
        "wlc_2017_to_2019",
        url=SOURCES["wlc_2017_to_2019"]["url"],
        filename=SOURCES["wlc_2017_to_2019"]["filename"],
    )
    wlc_11_zip = _download(
        "wlc_predev_to_2011",
        url=SOURCES["wlc_predev_to_2011"]["url"],
        filename=SOURCES["wlc_predev_to_2011"]["filename"],
    )

    log.info("extracting …")
    thick_dir = _extract(thick_zip)
    wlc_19_dir = _extract(wlc_19_zip)
    wlc_1719_dir = _extract(wlc_1719_zip)
    wlc_11_dir = _extract(wlc_11_zip)

    thick_raster = _find_raster(thick_dir)
    wlc_19_raster = _find_raster(wlc_19_dir)
    wlc_1719_raster = _find_raster(wlc_1719_dir)
    wlc_11_raster = _find_raster(wlc_11_dir)
    log.info("  thickness (2009):          %s", thick_raster)
    log.info("  water-level-change predev→2019: %s", wlc_19_raster)
    log.info("  water-level-change 2017→2019: %s", wlc_1719_raster)
    log.info("  water-level-change predev→2011 (legacy): %s", wlc_11_raster)

    counties = _load_counties()
    log.info("  %d HPA-state counties", len(counties))

    log.info("computing zonal-mean saturated thickness 2009 (feet, positive) …")
    thick = _zonal_mean(thick_raster, counties)
    thick["thickness_2009_m"] = thick["raster_mean"] * 0.3048
    thick = thick[["fips", "thickness_2009_m", "n_pixels"]].rename(columns={"n_pixels": "n_pixels_thick"})

    log.info("computing zonal-mean water-level change predev→2019 …")
    wlc_19 = _zonal_mean_signed(wlc_19_raster, counties)
    wlc_19["wlc_predev_2019_m"] = wlc_19["raster_mean"] * 0.3048
    wlc_19["annual_decline_m"] = wlc_19["wlc_predev_2019_m"] / WLC_YEARS_PRIMARY
    wlc_19 = wlc_19[["fips", "wlc_predev_2019_m", "annual_decline_m", "n_pixels"]].rename(
        columns={"n_pixels": "n_pixels_wlc19"}
    )

    log.info("computing zonal-mean water-level change 2017→2019 (recent 2-yr) …")
    wlc_biennium = _zonal_mean_signed(wlc_1719_raster, counties)
    wlc_biennium["decline_recent_m_yr"] = wlc_biennium["raster_mean"] * 0.3048 / 2.0
    wlc_biennium = wlc_biennium[["fips", "decline_recent_m_yr", "n_pixels"]].rename(
        columns={"n_pixels": "n_pixels_wlc_recent"}
    )

    log.info("computing zonal-mean water-level change predev→2011 (legacy for audit) …")
    wlc_11 = _zonal_mean_signed(wlc_11_raster, counties)
    wlc_11["wlc_predev_2011_m"] = wlc_11["raster_mean"] * 0.3048
    wlc_11["annual_decline_legacy_m"] = wlc_11["wlc_predev_2011_m"] / WLC_YEARS_LEGACY
    wlc_11 = wlc_11[["fips", "wlc_predev_2011_m", "annual_decline_legacy_m"]]

    # Derived 2019 thickness = 2009 + (wlc_predev_2019 − wlc_predev_2009)
    # where wlc_predev_2009 ≈ (2009/2011) × wlc_predev_2011 prorated.
    # Per McGuire & Strauch 2024 method note, section "Methods — 2019
    # thickness derivation": sum biennial 2009→19 changes onto the 2009 grid.
    # Since individual biennial rasters aren't all in our set, use the
    # 2017→2019 delta as the terminal step and back-project linearly for
    # 2009→2017 from the predev-to-2019 minus predev-to-2011 difference.
    merged = (
        thick.merge(wlc_19, on="fips", how="outer")
             .merge(wlc_biennium, on="fips", how="outer")
             .merge(wlc_11, on="fips", how="outer")
    )
    delta_2009_2019 = (
        merged["wlc_predev_2019_m"] - merged["wlc_predev_2011_m"] * (WLC_YEARS_LEGACY + 8) / WLC_YEARS_LEGACY
    )
    # Fallback: if legacy not available, approximate with (annual_decline × 10).
    fallback_delta = merged["annual_decline_m"] * 10
    delta_2009_2019 = delta_2009_2019.fillna(fallback_delta)
    merged["thickness_2019_m"] = merged["thickness_2009_m"] + delta_2009_2019

    # Primary output: modern (2019-era) thickness + 69-year annual decline.
    merged["thickness_m"] = merged["thickness_2019_m"].fillna(merged["thickness_2009_m"])
    merged["source"] = "usgs_mcguire_sir2023_5143"

    out = merged[[
        "fips",
        "thickness_m",              # 2019-derived thickness (m)
        "thickness_2009_m",         # 2009 published thickness (m)
        "thickness_2019_m",         # derived 2019 thickness (m)
        "annual_decline_m",         # predev→2019 / 69 yrs (m/yr)
        "annual_decline_legacy_m",  # predev→2011 / 61 yrs (m/yr, audit)
        "decline_recent_m_yr",      # 2017→2019 / 2 yrs (m/yr, recent)
        "n_pixels_thick", "n_pixels_wlc19", "n_pixels_wlc_recent",
        "source",
    ]]

    PROCESSED.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT, index=False)
    log.info("wrote %s  (%d counties)", OUTPUT.relative_to(DATA_DIR.parent), len(out))
    log.info(
        "  thickness_2019 median=%.1f m; decline predev→2019 median=%.3f m/yr; "
        "decline recent (2017→19) median=%.3f m/yr",
        out["thickness_m"].median() if out["thickness_m"].notna().any() else float("nan"),
        out["annual_decline_m"].median() if out["annual_decline_m"].notna().any() else float("nan"),
        out["decline_recent_m_yr"].median() if out["decline_recent_m_yr"].notna().any() else float("nan"),
    )
    return out


if __name__ == "__main__":
    build()
