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
   poetry install
   ```

2. **Copy the shared `.env`.** The parent project `Agricultural_Data_Analysis/.env` already has the AWS keys, DB URL, and USDA API keys we reuse. Copy it into `aquifer-watch/.env`. Verify with:
   ```bash
   poetry run python -c "from aquiferwatch.config import settings; print(settings.s3_bucket, settings.aws_region)"
   # -> usda-analysis-datasets us-east-2
   ```

3. **Fetch the data snapshot pinned in `DATA_VERSION`.**
   ```bash
   poetry run python scripts/fetch_data.py
   ```
   Downloads only files whose sha256 doesn't match the local copy. Land in `data/processed/`.

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

That pulls AWS + MLflow creds from your Colab **Secrets** panel into the process env. Set these secrets (gear icon → Secrets) once:

| Secret | Value |
|---|---|
| `AWS_ACCESS_KEY_ID` | from parent `.env` |
| `AWS_SECRET_ACCESS_KEY` | from parent `.env` |
| `AWS_REGION` | `us-east-2` |
| `S3_BUCKET` | `usda-analysis-datasets` |
| `MLFLOW_TRACKING_URI` | shared server (see [mlflow/README.md](../mlflow/README.md)) |
| `MLFLOW_TRACKING_USERNAME` | yours |
| `MLFLOW_TRACKING_PASSWORD` | yours |
| `AQW_DATA_VERSION` | optional; overrides `DATA_VERSION` file |

Then the first `load_baseline()` call transparently fetches the parquet from S3 into Colab's local storage.

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
