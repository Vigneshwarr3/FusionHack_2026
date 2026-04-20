"""Publish processed parquets to the shared S3 bucket.

Reuses the parent project's bucket (`usda-analysis-datasets`). Writes to both a
dated version prefix and `latest/`, and rewrites `DATA_VERSION` at repo root so
teammates who pull from git know what version is current.

Usage
-----
    poetry run python scripts/publish_data.py                  # default: all parquets
    poetry run python scripts/publish_data.py --dry-run        # preview
    poetry run python scripts/publish_data.py --only baseline  # one file

S3 layout
---------
    s3://usda-analysis-datasets/aquiferwatch/processed/
        latest/*.parquet                     <- symlink-style pointer
        v2026-04-20-01/*.parquet             <- dated snapshot
        v2026-04-20-01/MANIFEST.json         <- file list + sha256

AWS creds come from env (same set as parent project). If they're missing the
script exits with a clear message instead of silently failing.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
VERSION_FILE = REPO_ROOT / "DATA_VERSION"

BUCKET = os.getenv("S3_BUCKET", "usda-analysis-datasets")
PREFIX = "aquiferwatch/processed"
REGION = os.getenv("AWS_REGION", "us-east-2")


def _sha256(path: Path, buf: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(buf), b""):
            h.update(chunk)
    return h.hexdigest()


def _next_version(today: str) -> str:
    """Pick v{today}-NN where NN increments if today already has versions."""
    existing = VERSION_FILE.read_text().strip() if VERSION_FILE.exists() else ""
    if existing.startswith(f"v{today}-"):
        try:
            n = int(existing.rsplit("-", 1)[1]) + 1
        except ValueError:
            n = 1
    else:
        n = 1
    return f"v{today}-{n:02d}"


def publish(only: list[str] | None, dry_run: bool) -> None:
    if not PROCESSED_DIR.exists():
        sys.exit(f"no processed dir: {PROCESSED_DIR}")

    files = sorted(PROCESSED_DIR.glob("*.parquet"))
    if only:
        wanted = {name if name.endswith(".parquet") else f"{name}.parquet" for name in only}
        files = [f for f in files if f.name in wanted]
        missing = wanted - {f.name for f in files}
        if missing:
            sys.exit(f"not found: {sorted(missing)}")
    if not files:
        sys.exit("no parquets matched")

    today = datetime.utcnow().strftime("%Y-%m-%d")
    version = _next_version(today)
    total_mb = sum(f.stat().st_size for f in files) / 1024 / 1024

    print(f"bucket:  s3://{BUCKET}/{PREFIX}/")
    print(f"version: {version}")
    print(f"files:   {len(files)}  ({total_mb:.1f} MB)")
    if dry_run:
        for f in files:
            print(f"  [dry] {f.name:<50} {f.stat().st_size/1024/1024:>7.2f} MB")
        return

    try:
        s3 = boto3.client("s3", region_name=REGION)
    except NoCredentialsError:
        sys.exit("AWS creds not configured. Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.")

    manifest = {"version": version, "created_utc": datetime.utcnow().isoformat(), "files": []}

    for f in files:
        rel = f.name
        size_mb = f.stat().st_size / 1024 / 1024
        digest = _sha256(f)
        manifest["files"].append({"name": rel, "sha256": digest, "bytes": f.stat().st_size})

        for key in (f"{PREFIX}/{version}/{rel}", f"{PREFIX}/latest/{rel}"):
            print(f"  upload  {rel:<50} {size_mb:>7.2f} MB  ->  {key}")
            try:
                s3.upload_file(str(f), BUCKET, key)
            except (BotoCoreError, ClientError) as e:
                sys.exit(f"upload failed for {rel}: {e}")

    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
    for key in (f"{PREFIX}/{version}/MANIFEST.json", f"{PREFIX}/latest/MANIFEST.json"):
        print(f"  manifest -> {key}")
        s3.put_object(Bucket=BUCKET, Key=key, Body=manifest_bytes, ContentType="application/json")

    VERSION_FILE.write_text(version + "\n")
    print(f"\nwrote {VERSION_FILE.relative_to(REPO_ROOT)} = {version}")
    print("\ncommit the DATA_VERSION bump so teammates pick up the new snapshot:")
    print(f"  git add DATA_VERSION && git commit -m 'data: publish {version}'")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--only", nargs="+", help="restrict to named parquets (by filename)")
    p.add_argument("--dry-run", action="store_true", help="list files without uploading")
    args = p.parse_args()
    publish(args.only, args.dry_run)


if __name__ == "__main__":
    main()
