"""Fetch processed parquets from the shared S3 bucket.

Intended for teammates who don't run the ingest pipelines locally. Pulls the
version named in repo-root `DATA_VERSION` into `data/processed/`. Skips files
that already match by size + sha (per MANIFEST.json).

Usage
-----
    poetry run python scripts/fetch_data.py                    # DATA_VERSION (pinned)
    poetry run python scripts/fetch_data.py --version latest   # track latest
    poetry run python scripts/fetch_data.py --only baseline    # one file
    poetry run python scripts/fetch_data.py --list             # list available versions

AWS creds come from env (same set as parent project).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.client import Config
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


def _client():
    """S3 client for a public bucket: try credentialed first, fall back to unsigned.

    The bucket is public-read, so credentialed access isn't required \u2014 but if
    creds are available we use them (higher rate limits, clearer audit trail).
    """
    try:
        client = boto3.client("s3", region_name=REGION)
        client.list_objects_v2(Bucket=BUCKET, Prefix=f"{PREFIX}/", MaxKeys=1)
        return client
    except (NoCredentialsError, ClientError):
        return boto3.client("s3", region_name=REGION, config=Config(signature_version=UNSIGNED))


def _resolve_version(arg: str | None) -> str:
    if arg:
        return arg
    if VERSION_FILE.exists():
        v = VERSION_FILE.read_text().strip()
        if v:
            return v
    sys.exit("no DATA_VERSION file and no --version given")


def _list_versions() -> None:
    s3 = _client()
    paginator = s3.get_paginator("list_objects_v2")
    prefixes: set[str] = set()
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{PREFIX}/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []) or []:
            name = cp["Prefix"].removeprefix(f"{PREFIX}/").rstrip("/")
            if name:
                prefixes.add(name)
    if not prefixes:
        print(f"no versions under s3://{BUCKET}/{PREFIX}/")
        return
    for v in sorted(prefixes):
        print(v)


def fetch(version: str, only: list[str] | None) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    s3 = _client()
    version_prefix = f"{PREFIX}/{version}"

    # Load manifest to know expected hashes + skip-if-match
    manifest: dict | None = None
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"{version_prefix}/MANIFEST.json")
        manifest = json.loads(obj["Body"].read())
    except ClientError:
        print(f"warn: no MANIFEST.json at s3://{BUCKET}/{version_prefix}/ \u2014 downloading all files")

    if manifest:
        entries = manifest["files"]
        if only:
            wanted = {n if n.endswith(".parquet") else f"{n}.parquet" for n in only}
            entries = [e for e in entries if e["name"] in wanted]
        print(f"version: {version}  ({len(entries)} files)")
        for e in entries:
            dst = PROCESSED_DIR / e["name"]
            if dst.exists() and dst.stat().st_size == e["bytes"] and _sha256(dst) == e["sha256"]:
                print(f"  skip    {e['name']:<50} (matches)")
                continue
            key = f"{version_prefix}/{e['name']}"
            size_mb = e["bytes"] / 1024 / 1024
            print(f"  fetch   {e['name']:<50} {size_mb:>7.2f} MB")
            try:
                s3.download_file(BUCKET, key, str(dst))
            except (BotoCoreError, ClientError) as err:
                sys.exit(f"download failed for {e['name']}: {err}")
        print(f"\n\u2713 fetched into {PROCESSED_DIR.relative_to(REPO_ROOT)}/")
        return

    # No manifest \u2014 fall back to listing everything under the prefix
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{version_prefix}/"):
        for obj in page.get("Contents", []) or []:
            k = obj["Key"]
            if k.endswith(".parquet"):
                keys.append((k, obj["Size"]))
    if only:
        wanted = {n if n.endswith(".parquet") else f"{n}.parquet" for n in only}
        keys = [(k, s) for (k, s) in keys if Path(k).name in wanted]
    if not keys:
        sys.exit(f"no parquets under s3://{BUCKET}/{version_prefix}/")

    for k, size in keys:
        name = Path(k).name
        dst = PROCESSED_DIR / name
        size_mb = size / 1024 / 1024
        print(f"  fetch   {name:<50} {size_mb:>7.2f} MB")
        s3.download_file(BUCKET, k, str(dst))
    print(f"\n\u2713 fetched into {PROCESSED_DIR.relative_to(REPO_ROOT)}/")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--version", help="version tag (e.g. v2026-04-20-01 or 'latest'). Default: DATA_VERSION file.")
    p.add_argument("--only", nargs="+", help="restrict to named parquets (by filename)")
    p.add_argument("--list", action="store_true", help="list available versions and exit")
    args = p.parse_args()

    if args.list:
        _list_versions()
        return

    version = _resolve_version(args.version)
    fetch(version, args.only)


if __name__ == "__main__":
    main()
