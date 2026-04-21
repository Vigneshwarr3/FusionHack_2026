"""Build web/county_irrigation_history.json from Deines AIM-HPA annual acres.

Input:   data/processed/deines_annual_irrigated_acres.parquet
Output:  data/processed/web/county_irrigation_history.json
         {
           "version": "...",
           "min_year": 1984, "max_year": 2017,
           "counties": { "20055": [[1984, 12345], ...], ... },
           "aggregate": [[1984, <acres total HPA>], ...],
           "citation": "Deines et al. 2019 RSE 233: 111400"
         }

Usage
-----
    poetry run python scripts/build_irrigation_history.py
    poetry run python scripts/build_irrigation_history.py --upload
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd

from aquiferwatch.config import DATA_DIR

REPO_ROOT = DATA_DIR.parent
WEB_DIR = DATA_DIR / "processed" / "web"
OUTPUT = WEB_DIR / "county_irrigation_history.json"

BUCKET = os.getenv("S3_BUCKET", "usda-analysis-datasets")
S3_PREFIX = "aquiferwatch/web"

SOURCE_PARQUET = DATA_DIR / "processed" / "deines_annual_irrigated_acres.parquet"


def build() -> Path:
    if not SOURCE_PARQUET.exists():
        raise FileNotFoundError(
            f"{SOURCE_PARQUET} missing — run "
            "`poetry run python -m aquiferwatch.pipeline.deines_aim_hpa` first"
        )
    df = pd.read_parquet(SOURCE_PARQUET)
    df["fips"] = df["fips"].astype(str).str.zfill(5)
    df = df.sort_values(["fips", "year"])

    counties: dict[str, list[list[float]]] = {}
    for fips, sub in df.groupby("fips"):
        counties[fips] = [
            [int(y), round(float(a), 0)]
            for y, a in zip(sub["year"], sub["irrigated_acres_deines"])
            if pd.notna(a)
        ]
    # Aggregate across all HPA-footprint counties per year.
    agg = (
        df.groupby("year")["irrigated_acres_deines"]
          .sum()
          .round(0)
          .reset_index()
    )
    aggregate = [[int(y), float(a)] for y, a in zip(agg["year"], agg["irrigated_acres_deines"])]

    payload = {
        "version": pd.Timestamp.utcnow().isoformat(),
        "min_year": int(df["year"].min()),
        "max_year": int(df["year"].max()),
        "n_counties": len(counties),
        "citation": (
            "Deines, J.M., Kendall, A.D., Crowley, M.A., Rapp, J., Cardille, J.A., "
            "Hyndman, D.W., 2019. Mapping three decades of annual irrigation across "
            "the US High Plains Aquifer using Landsat and Google Earth Engine. "
            "Remote Sensing of Environment 233: 111400."
        ),
        "counties": counties,
        "aggregate": aggregate,
    }
    WEB_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(payload, separators=(",", ":")))
    size_kb = OUTPUT.stat().st_size / 1024
    print(f"wrote {OUTPUT.relative_to(REPO_ROOT)}  "
          f"({size_kb:.0f} KB, {len(counties)} counties × {len(aggregate)} years)")
    return OUTPUT


def upload(path: Path) -> None:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-2"))
    key = f"{S3_PREFIX}/{path.name}"
    print(f"upload {path.name} -> s3://{BUCKET}/{key}")
    try:
        s3.upload_file(str(path), BUCKET, key,
                       ExtraArgs={"ContentType": "application/json"})
    except (BotoCoreError, ClientError) as e:
        sys.exit(f"upload failed: {e}")
    print(f"  https://{BUCKET}.s3.amazonaws.com/{key}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--upload", action="store_true")
    args = p.parse_args()
    out = build()
    if args.upload:
        upload(out)


if __name__ == "__main__":
    main()
