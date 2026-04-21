"""Build web/county_history.json for the frontend drill-down sparkline.

Per-county annual saturated-thickness time series, assembled from the same
TWDB + WIZARD panel NB02 trains on. Counties without observations appear
with an empty `points` array — the UI should fall back to "no history".

Output schema (keeps the payload tight, no geometry):

    {
      "version": "<iso-timestamp>",
      "min_year": 1990,
      "max_year": 2025,
      "counties": {
        "20055": {
          "state": "KS",
          "county_name": "FINNEY",
          "source": "wizard",
          "points": [[1990, 22.5], [1991, 22.3], ...]
        },
        ...
      }
    }

Usage
-----
    poetry run python scripts/build_county_history.py
    poetry run python scripts/build_county_history.py --upload
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.data.real import build_depletion_history

REPO_ROOT = DATA_DIR.parent
WEB_DIR = DATA_DIR / "processed" / "web"
OUTPUT = WEB_DIR / "county_history.json"

BUCKET = os.getenv("S3_BUCKET", "usda-analysis-datasets")
S3_PREFIX = "aquiferwatch/web"


def build(min_year: int = 1990) -> Path:
    print(f"loading depletion panel (min_year={min_year})…")
    # Lower min_points threshold to 3 so even sparse counties get sparklines
    # — modeling needs 6+ pts, but a 3-point sparkline is still informative.
    history = build_depletion_history(min_year=min_year, min_points_per_county=3)
    print(f"  {history['fips'].nunique()} counties × {history['year'].nunique()} years  "
          f"({len(history)} rows)")

    # Map fips → state_name + county_name from baseline for UI labels.
    baseline = pd.read_parquet(DATA_DIR / "processed" / "baseline.parquet")
    baseline["fips"] = baseline["fips"].astype(str).str.zfill(5)
    labels = baseline.set_index("fips")[["state", "county_name"]].to_dict("index")

    history["fips"] = history["fips"].astype(str).str.zfill(5)
    history = history.sort_values(["fips", "year"])

    counties: dict[str, dict] = {}
    for fips, sub in history.groupby("fips"):
        points = [
            [int(y), round(float(t), 3)]
            for y, t in zip(sub["year"], sub["saturated_thickness_m"])
            if pd.notna(t)
        ]
        meta = labels.get(fips, {"state": None, "county_name": None})
        counties[fips] = {
            "state": meta["state"],
            "county_name": meta["county_name"],
            "source": sub["source"].iloc[0] if "source" in sub.columns else None,
            "points": points,
        }

    payload = {
        "version": pd.Timestamp.utcnow().isoformat(),
        "min_year": int(history["year"].min()),
        "max_year": int(history["year"].max()),
        "counties": counties,
    }

    WEB_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(payload, separators=(",", ":")))
    size_kb = OUTPUT.stat().st_size / 1024
    print(f"wrote {OUTPUT.relative_to(REPO_ROOT)}  ({size_kb:.1f} KB, {len(counties)} counties)")
    return OUTPUT


def upload(path: Path) -> None:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-2"))
    key = f"{S3_PREFIX}/{path.name}"
    print(f"upload {path.name} -> s3://{BUCKET}/{key}")
    try:
        s3.upload_file(
            str(path), BUCKET, key,
            ExtraArgs={"ContentType": "application/json"},
        )
    except (BotoCoreError, ClientError) as e:
        sys.exit(f"upload failed: {e}")
    print(f"public URL: https://{BUCKET}.s3.amazonaws.com/{key}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--upload", action="store_true")
    args = p.parse_args()

    out = build()
    if args.upload:
        upload(out)


if __name__ == "__main__":
    main()
