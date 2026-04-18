"""FastAPI router for AquiferWatch endpoints.

Designed to be MOUNTED into the parent Agricultural_Data_Analysis backend, so it
reuses CORS, RDS engine, and S3 client already configured there. Parent project
integrates via:

    from aquiferwatch.api.router import router as aquifer_router
    app.include_router(aquifer_router, prefix="/api/v1/predict/aquifer")

Routes (per spec §7):
  GET  /counties                    — county list w/ saturated thickness + depletion
  GET  /counties/{fips}/history     — per-county depletion time series
  GET  /scenarios                   — catalog of built-in scenarios
  POST /scenarios/{id}/run          — run a scenario, return ScenarioResult

A standalone dev server is also provided for local iteration before mounting.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from aquiferwatch.analytics.baseline import load_baseline
from aquiferwatch.analytics.scenarios import run_scenario
from aquiferwatch.schemas import (
    CustomScenarioParams,
    Scenario,
    ScenarioID,
    ScenarioResult,
)

router = APIRouter(tags=["aquifer"])


@router.get("/counties")
def list_counties() -> list[dict]:
    """List counties with current saturated thickness + years-until-uneconomic.

    Day-0: returns the current baseline as a record list so the frontend map
    layer has something to render. Replaced with a DB query once the real
    baseline is materialized.
    """
    baseline = load_baseline()
    return baseline.to_dict(orient="records")


@router.get("/counties/{fips}/history")
def county_history(fips: str) -> dict:
    raise HTTPException(status_code=501, detail="Day 2 — Raj owns")


@router.get("/scenarios", response_model=list[Scenario])
def list_scenarios() -> list[Scenario]:
    return [
        Scenario(
            id=ScenarioID.STATUS_QUO,
            display_name="Status Quo",
            description="Current extraction + crop mix continues.",
        ),
        Scenario(
            id=ScenarioID.KS_LEMA_AQUIFER_WIDE,
            display_name="Kansas LEMA, aquifer-wide",
            description="Sheridan-6 rules applied to every HPA county.",
            source="Basso et al. 2025",
        ),
        Scenario(
            id=ScenarioID.DRIP_TRANSITION,
            display_name="Drip irrigation transition",
            description="Center-pivot + flood → drip over 10 years.",
        ),
        Scenario(
            id=ScenarioID.CORN_REDUCTION_25,
            display_name="25% corn reduction",
            description="Corn acreage drops 25%; reallocated to sorghum/wheat/dryland.",
        ),
        Scenario(
            id=ScenarioID.NO_AG_BELOW_9M,
            display_name="No ag below 9m threshold",
            description="Counties below 9m saturated thickness stop pumping.",
        ),
        Scenario(
            id=ScenarioID.CUSTOM,
            display_name="Custom",
            description="User-adjustable sliders.",
        ),
    ]


@router.post("/scenarios/{scenario_id}/run", response_model=ScenarioResult)
def run_scenario_endpoint(
    scenario_id: ScenarioID,
    params: CustomScenarioParams | None = None,
) -> ScenarioResult:
    baseline = load_baseline()
    try:
        return run_scenario(scenario_id, baseline, params=params)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


# --- Standalone dev server (only used for iteration before mounting into parent) ---
def build_dev_app():
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware

    app = FastAPI(title="AquiferWatch (dev)")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(router, prefix="/api/v1/predict/aquifer")
    return app


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(build_dev_app(), host="0.0.0.0", port=8001)
