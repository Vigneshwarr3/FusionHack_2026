# Integration with Agricultural_Data_Analysis

AquiferWatch is a hackathon add-on to the parent project at `../Agricultural_Data_Analysis`. The integration path is designed so we don't fork the frontend or duplicate infra.

## Frontend-first integration (submission path, 2026-04-20)

The frontend talks directly to public-read S3 — **no FastAPI mount required for submission**. The full data layer lives under `s3://usda-analysis-datasets/aquiferwatch/web/`:

| Artifact | URL | Purpose |
|---|---|---|
| `baseline_counties.geojson` + manifest | `…/web/baseline_counties.geojson` | Choropleth source. Every county carries `saturated_thickness_m`, `annual_decline_m`, `thickness_source` ∈ {`wells`, `raster`, `fallback`, `none`}, `hpa_overlap_pct` + `overlap_area_km2` + `county_area_km2` (filter to HPA counties only), `annual_decline_m_pred` + `decline_lo_m` + `decline_hi_m` (NB02 CatBoost + 80% conformal, 89 counties), `years_until_uneconomic` (+ `_lo`, `_hi`), `decline_source` ∈ {`model`, `heuristic`}, plus crop/pumping/energy columns. `data_quality`: 227 `modeled_high`, 379 `no_data` (off-aquifer). |
| `county_history.json` | `…/web/county_history.json` | 90-county saturated-thickness time series (1990–2026) for the drill-down sparkline. |
| `scenarios/_index.json` + 5 scenario JSONs | `…/web/scenarios/{status_quo, ks_lema_aquifer_wide, drip_transition, corn_reduction_25, no_ag_below_9m}.json` | Precomputed `ScenarioResult` payloads — instant toggle without backend. |

Frontend hint: filter the GeoJSON features to `hpa_overlap_pct > 0` (or `data_quality != "no_data"`) to avoid painting fake green halos across east TX / west CO — 379 of 606 counties sit in HPA states but are NOT over the aquifer.

Headline numbers (after USGS raster fill + HPA overlap filter, 2026-04-20 evening): LEMA aquifer-wide → **+21.4y lifespan / −$0.05B ag**; drip transition → **+27.2y / ≈$0 ag**; 25% corn reduction → **−0.4y / −$0.09B** (basically neutral); no-ag-below-9m → **+0y / −$0.45B** (saves only the already-past-threshold counties).

## Backend integration (post-submission, optional)

**Pattern:** mount the AquiferWatch router into the parent FastAPI app.

In `Agricultural_Data_Analysis/backend/main.py`, after the existing routers are registered:

```python
from aquiferwatch.api.router import router as aquifer_router
app.include_router(aquifer_router, prefix="/api/v1/predict/aquifer")
```

This reuses:
- CORS middleware (same `allow_origins` as the existing dashboard)
- Async SQLAlchemy engine + RDS connection pool
- S3 client with existing AWS credentials
- Logging config, startup model-loading lifecycle

### Database migrations

Add via the parent project's Alembic setup (no new migration infra here):

- `aqw_wells`, `aqw_well_measurements`, `aqw_saturated_thickness` (time-series per county)
- `aqw_crop_extraction` (per county × crop × year)
- `aqw_depletion_forecasts` (per county × year forecast)
- `aqw_scenario_runs` (scenario id × run id × computed deltas)

**PostGIS:** run `CREATE EXTENSION IF NOT EXISTS postgis;` on `ag_dashboard` once. County polygons + well point geometries use native PostGIS types.

## Frontend integration

**Pattern:** new tab in the existing Next.js dashboard. *Add the tab only after model accuracy is validated — see order of operations below.*

Path: `Agricultural_Data_Analysis/web_app/src/components/aquifer/`, sibling to `forecasts/`, `crops/`, `market/`.

Add to the ViewMode switcher in `web_app/src/app/page.tsx`:

```typescript
type ViewMode = "crops" | "market" | "livestock" | "land-economy" | "forecasts" | "aquifer";
```

Recommended placement: standalone top-level tab (not nested inside Forecasts). The Ogallala story is distinct enough that a tab-level surface earns the polish; Forecasts is already crowded with price/acreage/yield.

### Frontend components that need to be NEW

- `AquiferMap.tsx` — deck.gl `GeoJsonLayer` (choropleth) + `ScatterplotLayer` (wells) + `PolygonLayer` (aquifer boundary). The parent's existing `maps/` folder uses ScatterplotLayer only; this is genuinely new work.
- `TimeScrubber.tsx` — 1950–2100 slider, drives map year state. Use Framer Motion for transitions.
- `ScenarioPanel.tsx` — scenario selector + stats panel + sliders for the custom scenario.
- `CountyDetailPanel.tsx` — click-a-county side drawer showing crop mix, extraction, $ per acre-ft, years-until-uneconomic.
- `MethodologyPage.tsx` — renders `docs/methodology.md` server-side.
- `FeaturedStory.tsx` — template for the three journalism-grade case pages.

### Frontend components that REUSE

- Design tokens (`web_app/src/utils/design.ts`)
- Chart primitives (Recharts wrappers for the supporting depletion curves)
- Data-fetching hook pattern (mirror `usePriceForecast.ts`)
- Dark theme + Cobbles & Currents typography

## Order of operations (per user's direction)

1. **Infra + data pipelines (Days 0–3):** ingestion, saturated-thickness interpolation, depletion projection, Texas imputation. All in this repo, no frontend touched. Acceptance: saturated-thickness R² ≥ 0.80 vs. Deines 2019 on Kansas; TX imputation MAE ≤ 20% of median pumpage. ✓ (2026-04-20)
2. **Scenario engine (Day 4):** pure Python, unit-tested, exposed via router. ✓ (34/34 tests passing)
3. **Frontend integration (Days 4–6):** once the above pass, Raj starts building the Next.js tab in the parent repo. Changed from "calling the mounted FastAPI" to **"reading precomputed S3 JSONs"** — see the frontend-first section above. Deck.gl tab shipped at parent `08b9ab5`; remaining panels (scenario panel, time scrubber, methodology page) are now UI-only work on the already-published data.
4. **Economic + emissions overlays, stories, polish, demo (Days 5–7).**

## Publish pipeline (run in order for a full web refresh)

```
poetry run python -m aquiferwatch.pipeline.hpa_overlap          # county × aquifer footprint
poetry run python -m aquiferwatch.pipeline.usgs_hpa_rasters     # McGuire rasters zonal mean
poetry run python -m aquiferwatch.pipeline.compose_baseline     # rebuild baseline.parquet
poetry run python scripts/persist_predictions.py                # NB02 CatBoost + conformal → county_predictions.parquet
poetry run python scripts/enrich_baseline.py                    # merge preds + years_until_uneconomic
poetry run python scripts/build_web_geojson.py --upload         # TIGER join + S3 publish
poetry run python scripts/build_county_history.py --upload      # sparkline JSON
poetry run python scripts/precompute_scenarios.py --upload      # 5 scenario JSONs + index
```

The first two pipelines only need to run if `hpa_boundary.parquet` or the McGuire sources change — cached outputs in `data/processed/` are stable. All scripts are idempotent. `--upload` requires an active `aws login` SSO session.

## MLflow integration

MLflow server is hosted in *this* repo's `mlflow/` setup. Both the parent project and AquiferWatch can point to the same tracking URI, but the parent project's existing pickle/JSON artifact convention doesn't need to change — we migrate only AquiferWatch runs to MLflow for Day 0 speed.
