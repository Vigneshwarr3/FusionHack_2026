# Data handoff — running the AquiferWatch notebooks on real data

Raj runs the ingest pipelines on his machine; Vignesh runs modeling in Colab or locally. Real parquets live in the shared S3 bucket (`usda-analysis-datasets`, same as parent Agricultural_Data_Analysis). This doc is the 10-minute setup.

## What's in S3

```
s3://usda-analysis-datasets/aquiferwatch/processed/
    latest/*.parquet                <- rolling pointer, always newest
    v2026-04-20-01/*.parquet        <- dated snapshots
    v<version>/MANIFEST.json        <- file list + sha256 per snapshot
```

Current snapshot: ~113 MB across 29 parquets covering all 8 HPA states. The version string pinned in git for teammates to reproduce Raj's exact run is written to [DATA_VERSION](../DATA_VERSION) at repo root.

## Local setup (once)

1. **Clone + install.**
   ```bash
   git clone https://github.com/Vigneshwarr3/FusionHack_2026.git aquifer-watch
   cd aquifer-watch

   python -m venv .venv
   .venv\Scripts\activate        # on Windows
   # source .venv/bin/activate   # on macOS/Linux

   pip install -r requirements.txt
   pip install -e . --no-deps    # install the aquiferwatch package itself
   ```

   `requirements.txt` is a `pip freeze` from Raj's working venv — 194 pinned
   packages tested end-to-end against the notebooks. Use `--no-deps` on the
   editable install so pip doesn't try to rebuild pyarrow from source.

   **Python version:** 3.13 or 3.14. On 3.14 some packages (pyarrow, scipy)
   only have wheels as of pyarrow 23 / scipy 1.17 — `requirements.txt` already
   pins those. If pip tries to build from source, upgrade pip first:
   `python -m pip install -U pip`.

2. **Fetch the data snapshot pinned in `DATA_VERSION`.**
   ```bash
   poetry run python scripts/fetch_data.py
   ```
   The S3 bucket is public-read, so no AWS keys are required. The fetch script
   falls back to anonymous access if creds aren't configured. Downloads only
   files whose sha256 doesn't match the local copy. Lands in `data/processed/`.

3. **(Optional) Set up `.env`** if you want to run ETL pipelines or point at
   non-default MLflow / USDA / NOAA endpoints. Not needed to run the notebooks.
   ```bash
   cp .env.example .env   # if one exists, or ask Raj for a copy
   ```

4. **(Optional) Confirm the notebook imports work.**
   ```bash
   poetry run python -c "
   from aquiferwatch.data.real import build_extraction_dataset, build_depletion_history, build_lagged_features
   print(build_extraction_dataset().shape)
   print(build_depletion_history().shape)
   "
   ```

## Colab setup (once per session)

Each notebook starts with:

```python
!pip -q install git+https://github.com/Vigneshwarr3/FusionHack_2026.git
from aquiferwatch.colab import bootstrap
bootstrap(team_member="vignesh")
```

The S3 bucket is public-read, so the first `load_baseline()` call works out of
the box \u2014 no Colab Secrets needed for data access. The only secrets worth
setting (gear icon \u2192 Secrets) are for MLflow, if you want your runs on the
shared tracking server:

| Secret | Value | Required for |
|---|---|---|
| `MLFLOW_TRACKING_URI` | shared server (see [mlflow/README.md](../mlflow/README.md)) | logged MLflow runs |
| `MLFLOW_TRACKING_USERNAME` | yours | same |
| `MLFLOW_TRACKING_PASSWORD` | yours | same |
| `AQW_DATA_VERSION` | e.g. `latest` or `v2026-04-20-01` | override the pinned DATA_VERSION |

## Running the notebooks

Both notebooks default to real data (`USE_REAL_DATA = True`). Flip to `False` at the top for synthetic smoke tests.

### `01_tx_extraction_imputation.ipynb`

- **Data:** 202 cross-sectional rows from `baseline.parquet` filtered to `data_quality == 'modeled_high'` — counties with measured saturated thickness and composed pumping.
- **Features:** crop acres (6 crops), saturated thickness, `n_wells`, grid kWh/AF.
- **Dropped vs. synthetic:** `precip_mm` (PRISM ingest deferred), `well_density` replaced by `n_wells`.
- **Limitation:** cross-sectional, not time panel. Panel arrives when the KS Open Records Request data lands.

### `02_depletion_projection.ipynb`

- **Data:** 3,248-row per-county annual thickness panel (90 counties × ~30 years), built from:
  - **TWDB TX wells** — `sat_thickness_m` precomputed in `twdb_water_levels.parquet`, aggregated to county × year median.
  - **KGS WIZARD KS wells** — thickness = `median_bed_depth_ft` (county snapshot) − `DEPTH_TO_WATER` (per measurement), aggregated similarly.
- **Snapshot covariates** (constant per county): `recharge_mm_yr`, `pumping_af_yr`, `annual_decline_m` from `baseline.parquet`.
- **Feature engineering:** `build_lagged_features()` produces `thickness_lag{1,2,3,5}`, `trend_5y`, `pumping_lag1`.

## When Raj pushes new data

1. Raj runs `poetry run python scripts/publish_data.py` — uploads to a new versioned prefix + `latest/`, rewrites `DATA_VERSION`.
2. Raj commits the `DATA_VERSION` bump.
3. Vignesh pulls the commit, reruns `poetry run python scripts/fetch_data.py`. Only changed files download.

## If you want to skip the version pin

Either set `AQW_DATA_VERSION=latest` in your env, or pass `--version latest` to `scripts/fetch_data.py`. Useful for tracking Raj's in-flight work; use the pinned version for reproducible experiments.

## MLflow

Every training run goes through `aquiferwatch.mlflow_utils.start_run`, which tags with `team_member`, `module`, `scenario`, `git_sha`. Filter the UI by `team_member=vignesh` to see only your runs. Notebook conventions: see [notebooks/README.md](../notebooks/README.md) and [mlflow/README.md](../mlflow/README.md).

## Gotchas

- **`data/processed/` is gitignored.** Never `git add` a parquet. Use `scripts/publish_data.py`.
- **Don't commit notebook outputs** — the `nbstripout` filter is installed by `poetry run nbstripout --install`.
- **`modeled_high` ≠ all counties with thickness.** It means thickness *and* decline measured. The 404 `modeled_low` rows have weaker estimates (bedrock × 0.5 or HPA median fallback). Train on `modeled_high` only; predict on `modeled_low` for imputation.
- **Panel for NB02 is KS + TX only today.** Adding NGWMN states means joining `ngwmn_water_levels.parquet` (1.68M rows) with `ngwmn_sites.parquet` for FIPS + a bedrock snapshot per state. Left as a follow-up in `aquiferwatch/data/real.py`.
