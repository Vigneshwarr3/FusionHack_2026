"""Publish curated reference tables to S3 for the frontend.

Reads `data/curated/*.json` and uploads each to
`s3://usda-analysis-datasets/aquiferwatch/web/curated/<filename>.json`.

Usage
-----
    poetry run python scripts/publish_curated.py --upload
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CURATED = REPO_ROOT / "data" / "curated"
WEB_DIR = REPO_ROOT / "data" / "processed" / "web" / "curated"

BUCKET = os.getenv("S3_BUCKET", "usda-analysis-datasets")
S3_PREFIX = "aquiferwatch/web/curated"


def stage() -> list[Path]:
    WEB_DIR.mkdir(parents=True, exist_ok=True)
    out: list[Path] = []
    for src in sorted(CURATED.glob("*.json")):
        dst = WEB_DIR / src.name
        shutil.copyfile(src, dst)
        size_kb = dst.stat().st_size / 1024
        print(f"  staged {dst.relative_to(REPO_ROOT)}  ({size_kb:.1f} KB)")
        out.append(dst)
    return out


def upload(paths: list[Path]) -> None:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-2"))
    for local in paths:
        key = f"{S3_PREFIX}/{local.name}"
        print(f"upload {local.name} -> s3://{BUCKET}/{key}")
        try:
            s3.upload_file(
                str(local), BUCKET, key,
                ExtraArgs={"ContentType": "application/json"},
            )
        except (BotoCoreError, ClientError) as e:
            sys.exit(f"upload failed: {e}")
        print(f"  https://{BUCKET}.s3.amazonaws.com/{key}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--upload", action="store_true")
    args = p.parse_args()
    paths = stage()
    if args.upload:
        upload(paths)


if __name__ == "__main__":
    main()
