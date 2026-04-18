"""Unit tests for the scenario engine.

Known-answer cases: every scenario is deterministic and its math is cited in
docs/scenarios.md. If a test fails, the code is wrong, not the test — don't
"fix" by relaxing the expected value.
"""

from __future__ import annotations

import pandas as pd
import pytest

from aquiferwatch.analytics.scenarios import (
    CROPS,
    METHOD_EFFICIENCY,
    REVENUE_PER_ACRE,
    THICKNESS_THRESHOLD_M,
    WATER_PER_ACRE,
    _years_until_uneconomic,
    run_scenario,
    validate_baseline,
)
from aquiferwatch.schemas import CustomScenarioParams, ScenarioID


def _make_baseline() -> pd.DataFrame:
    """Three-county toy baseline:
      - Finney KS: healthy thickness, active pumping
      - Dallam TX: near-threshold, heavy pumping
      - Hooker NE (Sandhills-like): thick, slow decline
    """
    rows = [
        dict(
            fips="20055", state="KS",
            saturated_thickness_m=22.0, annual_decline_m=-0.38, recharge_mm_yr=15.0,
            irrigated_acres_total=168_000,
            acres_corn=100_000, acres_soybeans=10_000, acres_sorghum=20_000,
            acres_wheat=25_000, acres_cotton=3_000, acres_alfalfa=10_000,
            irr_center_pivot=0.85, irr_flood=0.05, irr_drip=0.05, irr_dryland=0.05,
            kwh_per_af_pumped=250.0, grid_intensity_kg_per_kwh=0.45,
        ),
        dict(
            fips="48111", state="TX",
            saturated_thickness_m=7.5,  # already below 9 m
            annual_decline_m=-0.55, recharge_mm_yr=8.0,
            irrigated_acres_total=240_000,
            acres_corn=80_000, acres_soybeans=5_000, acres_sorghum=30_000,
            acres_wheat=20_000, acres_cotton=100_000, acres_alfalfa=5_000,
            irr_center_pivot=0.70, irr_flood=0.20, irr_drip=0.05, irr_dryland=0.05,
            kwh_per_af_pumped=320.0, grid_intensity_kg_per_kwh=0.52,
        ),
        dict(
            fips="31091", state="NE",
            saturated_thickness_m=180.0, annual_decline_m=-0.05, recharge_mm_yr=95.0,
            irrigated_acres_total=85_000,
            acres_corn=40_000, acres_soybeans=25_000, acres_sorghum=5_000,
            acres_wheat=10_000, acres_cotton=0, acres_alfalfa=5_000,
            irr_center_pivot=0.95, irr_flood=0.00, irr_drip=0.05, irr_dryland=0.00,
            kwh_per_af_pumped=180.0, grid_intensity_kg_per_kwh=0.62,
        ),
    ]
    df = pd.DataFrame(rows)
    # Derive revenue and pumping from the crop mix so counties are internally
    # consistent with how scenarios recompute these. Pumping uses the
    # irrigation-method efficiency factor to match `_pumping_from_crop_mix`.
    df["ag_value_usd"] = sum(df[f"acres_{c}"] * REVENUE_PER_ACRE[c] for c in CROPS)
    base_water = sum(df[f"acres_{c}"] * WATER_PER_ACRE[c] for c in CROPS)
    eff = (
        df["irr_center_pivot"] * METHOD_EFFICIENCY["center_pivot"]
        + df["irr_flood"] * METHOD_EFFICIENCY["flood"]
        + df["irr_drip"] * METHOD_EFFICIENCY["drip"]
        + df["irr_dryland"] * METHOD_EFFICIENCY["dryland"]
    )
    df["pumping_af_yr"] = base_water * eff
    df["employment_fte"] = df["ag_value_usd"] * (0.021 / 1000.0)
    return df


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------
def test_validate_rejects_missing_column():
    b = _make_baseline().drop(columns=["pumping_af_yr"])
    with pytest.raises(ValueError, match="missing columns"):
        validate_baseline(b)


def test_validate_rejects_duplicate_fips():
    b = pd.concat([_make_baseline(), _make_baseline().head(1)], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate FIPS"):
        validate_baseline(b)


# ---------------------------------------------------------------------------
# Years-until-uneconomic edge cases
# ---------------------------------------------------------------------------
def test_years_until_uneconomic_depleting():
    yrs = _years_until_uneconomic(
        pd.Series([22.0]), pd.Series([-0.38])
    )
    # (22 - 9) / 0.38 ≈ 34.2
    assert 33.5 < yrs.iloc[0] < 35.0


def test_years_until_uneconomic_already_below_threshold():
    yrs = _years_until_uneconomic(
        pd.Series([7.5]), pd.Series([-0.55])
    )
    assert yrs.iloc[0] == 0.0


def test_years_until_uneconomic_stable_is_capped():
    yrs = _years_until_uneconomic(
        pd.Series([180.0]), pd.Series([0.0])  # no decline
    )
    assert yrs.iloc[0] == 1000.0


def test_years_until_uneconomic_rising_is_capped():
    yrs = _years_until_uneconomic(
        pd.Series([50.0]), pd.Series([0.5])  # rising
    )
    assert yrs.iloc[0] == 1000.0


# ---------------------------------------------------------------------------
# Scenario 1 — status quo (identity)
# ---------------------------------------------------------------------------
def test_status_quo_has_zero_deltas():
    r = run_scenario(ScenarioID.STATUS_QUO, _make_baseline())
    assert r.aquifer_lifespan_extension_years == pytest.approx(0.0, abs=1e-6)
    assert r.cumulative_ag_production_delta_usd_b == pytest.approx(0.0, abs=1e-9)
    assert r.embedded_co2_delta_mt == pytest.approx(0.0, abs=1e-6)
    for d in r.per_county:
        assert d.years_until_uneconomic_delta == pytest.approx(0.0, abs=1e-6)
        assert d.ag_value_delta_usd == pytest.approx(0.0, abs=1e-6)
        assert d.co2_delta_mt == pytest.approx(0.0, abs=1e-6)


# ---------------------------------------------------------------------------
# Scenario 2 — KS LEMA aquifer-wide
# ---------------------------------------------------------------------------
def test_ks_lema_reduces_pumping_and_extends_lifespan():
    r = run_scenario(ScenarioID.KS_LEMA_AQUIFER_WIDE, _make_baseline())
    # Lifespan should extend (less pumping → slower decline → more years).
    assert r.aquifer_lifespan_extension_years > 0
    # Less pumping → negative CO2 delta (emissions avoided).
    assert r.embedded_co2_delta_mt < 0


# ---------------------------------------------------------------------------
# Scenario 3 — drip transition
# ---------------------------------------------------------------------------
def test_drip_transition_reduces_pumping():
    r = run_scenario(ScenarioID.DRIP_TRANSITION, _make_baseline())
    # Drip is more efficient than center-pivot/flood → less pumping
    # → negative CO2 delta, positive lifespan extension.
    assert r.embedded_co2_delta_mt < 0
    assert r.aquifer_lifespan_extension_years > 0


# ---------------------------------------------------------------------------
# Scenario 4 — 25% corn reduction
# ---------------------------------------------------------------------------
def test_corn_reduction_shifts_mix_and_reduces_water():
    r = run_scenario(ScenarioID.CORN_REDUCTION_25, _make_baseline())
    # Corn uses 1.40 ac-ft/acre; sorghum 0.80, wheat 0.70. Even ignoring
    # dryland, the new mix uses less water → less pumping → negative CO2.
    assert r.embedded_co2_delta_mt < 0
    # Ag value usually drops (sorghum/wheat less valuable per acre than corn),
    # but this is county-specific — just check the aggregate is not positive.
    assert r.cumulative_ag_production_delta_usd_b <= 0


# ---------------------------------------------------------------------------
# Scenario 5 — no ag below 9 m
# ---------------------------------------------------------------------------
def test_no_ag_below_threshold_zeros_out_dallam():
    r = run_scenario(ScenarioID.NO_AG_BELOW_9M, _make_baseline())
    # Dallam TX is at 7.5 m — should go to zero pumping and zero ag.
    dallam = next(d for d in r.per_county if d.fips == "48111")
    # Pumping delta for Dallam is fully negative; ag delta large and negative.
    assert dallam.ag_value_delta_usd < 0
    assert dallam.co2_delta_mt < 0
    # Finney and Hooker (above threshold) should be unchanged.
    finney = next(d for d in r.per_county if d.fips == "20055")
    hooker = next(d for d in r.per_county if d.fips == "31091")
    assert finney.ag_value_delta_usd == pytest.approx(0.0, abs=1e-6)
    assert hooker.ag_value_delta_usd == pytest.approx(0.0, abs=1e-6)


# ---------------------------------------------------------------------------
# Scenario 6 — custom
# ---------------------------------------------------------------------------
def test_custom_with_zero_params_is_status_quo():
    r = run_scenario(
        ScenarioID.CUSTOM, _make_baseline(),
        params=CustomScenarioParams(
            pumping_reduction_pct=0.0,
            corn_to_sorghum_shift_pct=0.0,
            drip_adoption_pct=0.0,
        ),
    )
    assert r.cumulative_ag_production_delta_usd_b == pytest.approx(0.0, abs=1e-9)
    assert r.embedded_co2_delta_mt == pytest.approx(0.0, abs=1e-6)


def test_custom_pumping_reduction_scales_linearly():
    r10 = run_scenario(
        ScenarioID.CUSTOM, _make_baseline(),
        params=CustomScenarioParams(pumping_reduction_pct=0.10),
    )
    r20 = run_scenario(
        ScenarioID.CUSTOM, _make_baseline(),
        params=CustomScenarioParams(pumping_reduction_pct=0.20),
    )
    # 20% cut should produce roughly 2× the CO2 reduction of a 10% cut.
    ratio = r20.embedded_co2_delta_mt / r10.embedded_co2_delta_mt
    assert 1.9 < ratio < 2.1


def test_custom_missing_params_raises():
    with pytest.raises(ValueError, match="CUSTOM scenario requires"):
        run_scenario(ScenarioID.CUSTOM, _make_baseline(), params=None)


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------
def test_same_input_same_output():
    b = _make_baseline()
    r1 = run_scenario(ScenarioID.KS_LEMA_AQUIFER_WIDE, b)
    r2 = run_scenario(ScenarioID.KS_LEMA_AQUIFER_WIDE, b)
    assert r1.aquifer_lifespan_extension_years == r2.aquifer_lifespan_extension_years
    assert r1.embedded_co2_delta_mt == r2.embedded_co2_delta_mt
