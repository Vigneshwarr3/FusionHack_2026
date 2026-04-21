# AquiferWatch

County-level accountability platform for the Ogallala / High Plains Aquifer across 8 states. Built as a hackathon add-on to the existing [Agricultural_Data_Analysis](../Agricultural_Data_Analysis) project.

See [`aquiferwatch_spec.md`](aquiferwatch_spec.md) for the full spec.

---

## What lives in this repo vs. the parent project

**Here (new work):**
- Aquifer-specific data ingestion (USGS wells, KGS WIMAS, TX TWDB, NE DNR)
- Saturated-thickness interpolation & depletion projection models
- Texas extraction imputation (LightGBM)
- Scenario engine (policy counterfactuals)
- Methodology docs
- MLflow experiment tracking

**Reused from `Agricultural_Data_Analysis/` (not duplicated):**
- USDA NASS QuickStats ingestion patterns → `pipeline/quickstats_ingest.py`
- NOAA / NASA POWER / drought ETL patterns → `backend/etl/`
- FastAPI backend, auth, CORS, S3 client, RDS connection → `backend/main.py`
- Pydantic schema + SQLAlchemy patterns → `backend/models/`
- Next.js 14 + deck.gl + Tailwind frontend → `web_app/` (AquiferWatch tab added there)
- S3 bucket `usda-analysis-datasets`, RDS `ag-dashboard`, EC2 deployment

**Integration points:** see [`docs/integration.md`](docs/integration.md).

---

## Quickstart

```bash
# 1. Install deps
poetry install

# 2. Copy env and fill in shared creds (same DATABASE_URL, AWS, etc. as parent project)
cp .env.example .env

# 3. Smoke-test
poetry run pytest
poetry run python -m aquiferwatch.pipeline.usgs_wells --smoke

# 4. Start MLflow (local Day 0; team mode documented in mlflow/README.md)
poetry run mlflow ui --backend-store-uri sqlite:///mlflow.db --port 5000
```

---

## Team workflow

- Every notebook must import from `aquiferwatch.*` packages, not inline code. Logic lives in the package; notebooks are thin drivers. See [`notebooks/README.md`](notebooks/README.md).
- Every model training run logs to MLflow via `aquiferwatch.mlflow_utils.start_run(...)` with standard tags (`team_member`, `module`, `scenario`). This is how we stay visible to each other.
- Branch per module. Open PRs against `main`.

---

## Repo layout

```
aquifer-watch/
├── aquiferwatch/            # Python package — all logic lives here
│   ├── schemas.py           # County, AquiferSection, CropWaterFootprint, Scenario, ScenarioResult
│   ├── config.py            # Settings (MLflow URI, RDS, S3, API keys)
│   ├── mlflow_utils.py      # Shared experiment-tracking wrapper
│   ├── pipeline/            # Ingest: usgs_wells, kgs_wimas, tx_twdb, ne_dnr, usda_irrigation, common
│   ├── analytics/           # saturated_thickness, depletion, extraction_imputation, scenarios, economics
│   └── api/                 # FastAPI router (mountable into parent backend)
├── notebooks/               # Thin drivers — call aquiferwatch.* functions, log to MLflow
├── docs/                    # methodology, data_sources, limitations, scenarios, integration
├── scripts/                 # Orchestration: fetch/publish data, build web artifacts, precompute scenarios
├── tests/                   # pytest
├── data/                    # raw/, interim/, processed/ — raw/interim gitignored
├── mlflow/                  # MLflow server config + team setup notes
└── aquiferwatch_spec.md     # full hackathon spec
```

## Full web refresh

```bash
poetry run python -m aquiferwatch.pipeline.hpa_overlap        # county × aquifer footprint (new)
poetry run python -m aquiferwatch.pipeline.usgs_hpa_rasters   # McGuire rasters zonal mean (new)
poetry run python -m aquiferwatch.pipeline.compose_baseline
poetry run python scripts/persist_predictions.py              # NB02 CatBoost + conformal bands
poetry run python scripts/enrich_baseline.py                  # join preds + years_until_uneconomic
poetry run python scripts/build_web_geojson.py --upload
poetry run python scripts/build_county_history.py --upload
poetry run python scripts/precompute_scenarios.py --upload
```

Publishes to `s3://usda-analysis-datasets/aquiferwatch/web/` (public-read). The frontend reads these JSONs directly — no live FastAPI required for the submission path. See [`docs/integration.md`](docs/integration.md) for the artifact contract.
