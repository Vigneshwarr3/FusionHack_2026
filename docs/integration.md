# Integration with Agricultural_Data_Analysis

AquiferWatch is a hackathon add-on to the parent project at `../Agricultural_Data_Analysis`. The integration path is designed so we don't fork the frontend or duplicate infra.

## Backend integration

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

1. **Infra + data pipelines (Days 0–3):** ingestion, saturated-thickness interpolation, depletion projection, Texas imputation. All in this repo, no frontend touched. Acceptance: saturated-thickness R² ≥ 0.80 vs. Deines 2019 on Kansas; TX imputation MAE ≤ 20% of median pumpage.
2. **Scenario engine (Day 4):** pure Python, unit-tested, exposed via router.
3. **Frontend integration (Days 4–6):** once the above pass, Raj starts building the Next.js tab in the parent repo, calling the mounted `/api/v1/predict/aquifer/*` routes.
4. **Economic + emissions overlays, stories, polish, demo (Days 5–7).**

## MLflow integration

MLflow server is hosted in *this* repo's `mlflow/` setup. Both the parent project and AquiferWatch can point to the same tracking URI, but the parent project's existing pickle/JSON artifact convention doesn't need to change — we migrate only AquiferWatch runs to MLflow for Day 0 speed.
