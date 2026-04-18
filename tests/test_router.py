"""End-to-end tests for the FastAPI router against the scenario engine."""

from __future__ import annotations

from fastapi.testclient import TestClient

from aquiferwatch.api.router import build_dev_app


def _client() -> TestClient:
    return TestClient(build_dev_app())


def test_scenarios_catalog():
    client = _client()
    resp = client.get("/api/v1/predict/aquifer/scenarios")
    assert resp.status_code == 200
    names = {s["id"] for s in resp.json()}
    assert {"status_quo", "ks_lema_aquifer_wide", "drip_transition",
            "corn_reduction_25", "no_ag_below_9m", "custom"} == names


def test_counties_endpoint_returns_baseline():
    client = _client()
    resp = client.get("/api/v1/predict/aquifer/counties")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) >= 3
    assert {"fips", "state", "saturated_thickness_m"}.issubset(data[0].keys())


def test_run_status_quo_returns_zero_deltas():
    client = _client()
    resp = client.post("/api/v1/predict/aquifer/scenarios/status_quo/run")
    assert resp.status_code == 200
    body = resp.json()
    assert body["scenario_id"] == "status_quo"
    assert body["embedded_co2_delta_mt"] == 0.0


def test_run_ks_lema():
    client = _client()
    resp = client.post("/api/v1/predict/aquifer/scenarios/ks_lema_aquifer_wide/run")
    assert resp.status_code == 200
    body = resp.json()
    # Less pumping → emissions down
    assert body["embedded_co2_delta_mt"] < 0
    assert body["aquifer_lifespan_extension_years"] > 0


def test_run_custom_with_params():
    client = _client()
    resp = client.post(
        "/api/v1/predict/aquifer/scenarios/custom/run",
        json={
            "pumping_reduction_pct": 0.15,
            "corn_to_sorghum_shift_pct": 0.0,
            "drip_adoption_pct": 0.0,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["embedded_co2_delta_mt"] < 0


def test_run_custom_without_params_400():
    client = _client()
    # FastAPI will treat missing body as None for Optional params.
    # The engine raises ValueError, which we map to 400.
    resp = client.post("/api/v1/predict/aquifer/scenarios/custom/run")
    assert resp.status_code == 400
