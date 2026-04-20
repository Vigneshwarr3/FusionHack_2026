"""S3-aware parquet loader for notebooks and package code.

One rule: always call these loaders instead of reading `data/processed/*.parquet`
directly. That way a teammate who hasn't run the local pipelines still works
\u2014 the loader transparently fetches missing files from the shared S3 bucket
and caches them locally.

Resolution order for each parquet:
    1. `data/processed/<name>.parquet` in the repo (fast path; skip S3)
    2. Download from `s3://<S3_BUCKET>/aquiferwatch/processed/<version>/<name>.parquet`
       where `<version>` is whatever is pinned in `DATA_VERSION` at repo root,
       or the `AQW_DATA_VERSION` env var if set, or `latest`.
    3. Raise with a clear message if neither works.

Example
-------
    from aquiferwatch.data.loaders import load_baseline, load_thickness_panel
    baseline = load_baseline()            # 606-row cross-sectional
    panel    = load_thickness_panel()     # multi-year well-level thickness
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import pandas as pd

from aquiferwatch.config import DATA_DIR, settings

PROCESSED_DIR = DATA_DIR / "processed"
VERSION_FILE = DATA_DIR.parent / "DATA_VERSION"
S3_PREFIX = "aquiferwatch/processed"


def _resolve_version() -> str:
    env = os.getenv("AQW_DATA_VERSION")
    if env:
        return env
    if VERSION_FILE.exists():
        v = VERSION_FILE.read_text().strip()
        if v:
            return v
    return "latest"


def _fetch_from_s3(name: str, dst: Path) -> None:
    """Download one parquet from S3 into `dst`. Raises with a useful message on failure."""
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
    except ImportError as e:
        raise RuntimeError(
            f"Can't fetch {name} from S3 because boto3 isn't installed. "
            "Run `poetry install` or `pip install boto3`."
        ) from e

    version = _resolve_version()
    key = f"{S3_PREFIX}/{version}/{name}"
    bucket = settings.s3_bucket

    try:
        s3 = boto3.client("s3", region_name=settings.aws_region)
        dst.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, key, str(dst))
    except NoCredentialsError as e:
        raise RuntimeError(
            f"Can't fetch s3://{bucket}/{key}: no AWS credentials. "
            "Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in .env "
            "(reuse the values from Agricultural_Data_Analysis/.env)."
        ) from e
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(
            f"Failed to download s3://{bucket}/{key}. "
            f"Is the DATA_VERSION ({version}) published? Error: {e}"
        ) from e


def load_parquet(name: str) -> pd.DataFrame:
    """Load a processed parquet by filename. Fetches from S3 on cache miss."""
    if not name.endswith(".parquet"):
        name = f"{name}.parquet"
    local = PROCESSED_DIR / name
    if not local.exists():
        _fetch_from_s3(name, local)
    return pd.read_parquet(local)


# ---------------------------------------------------------------------------
# Named loaders \u2014 add one per parquet that notebooks / package code touch.
# Caching: small tables can be memoized; big ones stay uncached to keep RAM low.
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def load_baseline() -> pd.DataFrame:
    """606-row cross-sectional baseline (one per HPA-state county).

    Columns include: fips, state, county_name, acres_{crop}, saturated_thickness_m,
    annual_decline_m, pumping_af_yr, data_quality. See `compose_baseline.py`.
    """
    return load_parquet("baseline.parquet")


@lru_cache(maxsize=1)
def load_nass_irrigated() -> pd.DataFrame:
    """USDA NASS irrigated acres panel (fips, year, crop, value)."""
    return load_parquet("nass_irrigated_acres.parquet")


@lru_cache(maxsize=1)
def load_iwms_rates() -> pd.DataFrame:
    """USDA IWMS acre-feet per irrigated acre by state + crop."""
    return load_parquet("iwms_water_per_acre.parquet")


@lru_cache(maxsize=1)
def load_eia_grid_intensity() -> pd.DataFrame:
    """EIA grid CO2 intensity by state and year."""
    return load_parquet("eia_grid_intensity.parquet")


@lru_cache(maxsize=1)
def load_ers_revenue() -> pd.DataFrame:
    """USDA ERS revenue per acre (national average, by crop)."""
    return load_parquet("ers_revenue_per_acre.parquet")


# --- Water-level time series (used to build depletion panel) ---------------

def load_wizard_levels() -> pd.DataFrame:
    """KGS WIZARD well-level DEPTH_TO_WATER time series (KS only, ~601k rows)."""
    return load_parquet("kgs_wizard_levels.parquet")


def load_wizard_sites() -> pd.DataFrame:
    """WIZARD site metadata (fips per well)."""
    return load_parquet("kgs_wizard_sites.parquet")


def load_ngwmn_levels() -> pd.DataFrame:
    """NGWMN well-level time series (multi-state, ~1.68M rows)."""
    return load_parquet("ngwmn_water_levels.parquet")


def load_ngwmn_sites() -> pd.DataFrame:
    """NGWMN site metadata (fips per well)."""
    return load_parquet("ngwmn_sites.parquet")


def load_twdb_levels() -> pd.DataFrame:
    """TWDB TX well-level time series with pre-computed sat_thickness_m (~239k rows)."""
    return load_parquet("twdb_water_levels.parquet")


def load_twdb_wells() -> pd.DataFrame:
    """TWDB well metadata (StateWellNumber, County, lat, lon)."""
    return load_parquet("twdb_wells.parquet")


def load_ne_dnr_wells() -> pd.DataFrame:
    """Nebraska DNR well registry (~680k wells)."""
    return load_parquet("ne_dnr_wells.parquet")


def load_ne_dnr_county_summary() -> pd.DataFrame:
    """Nebraska DNR county-level snapshot (93 counties)."""
    return load_parquet("ne_dnr_county_summary.parquet")
