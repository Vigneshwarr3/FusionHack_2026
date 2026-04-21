"""Build a web-ready GeoJSON snapshot for the deck.gl frontend.

Fetches county polygons from US Census TIGER cartographic boundaries
(cb_2022_us_county_500k), filters to the 8 HPA states, joins with
`baseline.parquet`, simplifies geometries for web-payload size, writes
to `data/processed/web/baseline_counties.geojson`, and (with --upload)
pushes it to s3://<bucket>/aquiferwatch/web/ where the frontend can
fetch it directly — no backend call required for the initial render.

Usage
-----
    poetry run python scripts/build_web_geojson.py
    poetry run python scripts/build_web_geojson.py --upload
    poetry run python scripts/build_web_geojson.py --tolerance 0.005 --upload

The tolerance controls Douglas-Peucker simplification in degrees.
0.005 (~500m at HPA latitudes) gives a tight web-sized payload while
keeping county boundaries visually recognizable.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED = REPO_ROOT / "data" / "processed"
WEB_DIR = PROCESSED / "web"
RAW_DIR = REPO_ROOT / "data" / "raw" / "tiger"

TIGER_URL = (
    "https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_county_500k.zip"
)
HPA_STATE_FIPS = {"08", "20", "31", "35", "40", "46", "48", "56"}  # CO KS NE NM OK SD TX WY

BUCKET = os.getenv("S3_BUCKET", "usda-analysis-datasets")
S3_PREFIX = "aquiferwatch/web"


def fetch_counties() -> gpd.GeoDataFrame:
    """Download cb_2022_us_county_500k once; cache the .zip locally."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = RAW_DIR / "cb_2022_us_county_500k.zip"
    if not zip_path.exists():
        print(f"  downloading {TIGER_URL}")
        r = requests.get(TIGER_URL, timeout=120)
        r.raise_for_status()
        zip_path.write_bytes(r.content)
        print(f"  cached ({len(r.content) / 1024 / 1024:.1f} MB) -> {zip_path}")
    else:
        print(f"  using cached {zip_path.name}")

    # geopandas reads shp directly from inside a zip
    gdf = gpd.read_file(f"zip://{zip_path}")
    return gdf


def build(tolerance: float) -> Path:
    print("loading baseline.parquet...")
    baseline = pd.read_parquet(PROCESSED / "baseline.parquet")
    baseline["fips"] = baseline["fips"].astype(str).str.zfill(5)
    print(f"  {len(baseline)} counties, data_quality: {baseline['data_quality'].value_counts().to_dict()}")

    print("fetching TIGER county polygons...")
    counties = fetch_counties()
    counties["fips"] = (counties["STATEFP"].astype(str) + counties["COUNTYFP"].astype(str)).str.zfill(5)
    counties = counties[counties["STATEFP"].isin(HPA_STATE_FIPS)][["fips", "NAME", "geometry"]]
    print(f"  {len(counties)} counties across 8 HPA states")

    print("joining + simplifying...")
    merged = counties.merge(baseline, on="fips", how="inner")
    print(f"  {len(merged)} counties joined")

    merged["geometry"] = merged["geometry"].simplify(tolerance=tolerance, preserve_topology=True)

    # Trim to just the fields a map UI actually needs; keep everything lean.
    keep = [
        "fips", "state", "state_name", "county_name",
        "saturated_thickness_m", "annual_decline_m", "recharge_mm_yr",
        # HPA footprint — lets the frontend filter / dim non-aquifer counties
        # so we don't paint a fake halo across east TX / west CO.
        "hpa_overlap_pct", "overlap_area_km2", "county_area_km2",
        "thickness_source",  # 'wells' | 'raster' | 'fallback' | 'none'
        # Model-derived fields (see scripts/enrich_baseline.py). `decline_source`
        # flags counties without a model prediction so the UI can dim them.
        "annual_decline_m_pred", "decline_lo_m", "decline_hi_m",
        "thickness_pred_next_m", "decline_source",
        "years_until_uneconomic", "years_until_uneconomic_lo", "years_until_uneconomic_hi",
        "model_id", "coverage_target",
        "pumping_af_yr", "pumping_af_yr_usgs2015", "irrigated_acres_total",
        "acres_corn", "acres_soybeans", "acres_sorghum", "acres_wheat",
        "acres_cotton", "acres_alfalfa",
        "ag_value_usd", "kwh_per_af_pumped", "grid_intensity_kg_per_kwh",
        "n_wells", "data_quality", "geometry",
    ]
    out = merged[[c for c in keep if c in merged.columns]].copy()

    WEB_DIR.mkdir(parents=True, exist_ok=True)
    out_path = WEB_DIR / "baseline_counties.geojson"
    out.to_file(out_path, driver="GeoJSON")
    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"wrote {out_path.relative_to(REPO_ROOT)}  ({size_mb:.2f} MB)")

    manifest = {
        "version": pd.Timestamp.utcnow().isoformat(),
        "counties": len(out),
        "states": sorted(out["state"].unique().tolist()),
        "source_parquet": "baseline.parquet",
        "tolerance_deg": tolerance,
        "crs": "EPSG:4326",
        "fields": [c for c in keep if c in out.columns and c != "geometry"],
        "data_quality_counts": out["data_quality"].value_counts().to_dict(),
    }
    manifest_path = WEB_DIR / "baseline_counties.manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"wrote {manifest_path.relative_to(REPO_ROOT)}")

    return out_path


def upload(geojson_path: Path) -> None:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-2"))
    manifest_path = geojson_path.with_suffix("").with_suffix(".manifest.json")
    for local in (geojson_path, manifest_path):
        key = f"{S3_PREFIX}/{local.name}"
        print(f"upload {local.name} -> s3://{BUCKET}/{key}")
        try:
            extra = {"ContentType": "application/geo+json" if local.suffix == ".geojson" else "application/json"}
            s3.upload_file(str(local), BUCKET, key, ExtraArgs=extra)
        except (BotoCoreError, ClientError) as e:
            sys.exit(f"upload failed: {e}")
    print()
    print("public URLs (bucket is public-read at object level):")
    print(f"  https://{BUCKET}.s3.amazonaws.com/{S3_PREFIX}/{geojson_path.name}")
    print(f"  https://{BUCKET}.s3.amazonaws.com/{S3_PREFIX}/{manifest_path.name}")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--tolerance", type=float, default=0.005, help="Douglas-Peucker tolerance in degrees (default 0.005 ~ 500m)")
    p.add_argument("--upload", action="store_true", help="push the geojson + manifest to S3")
    args = p.parse_args()

    out = build(args.tolerance)
    if args.upload:
        upload(out)


if __name__ == "__main__":
    main()
