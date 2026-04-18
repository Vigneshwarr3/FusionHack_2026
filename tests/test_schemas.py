"""Smoke tests for the Day-0 Pydantic contracts."""

from datetime import date

import pytest
from pydantic import ValidationError

from aquiferwatch.schemas import (
    AquiferSection,
    County,
    CountyScenarioDelta,
    CropWaterFootprint,
    CustomScenarioParams,
    DataQuality,
    ScenarioID,
    ScenarioResult,
)


def test_county_fips_validation():
    County(
        fips="20055",
        state="KS",
        name="Finney",
        aquifer_overlap_pct=0.92,
        data_quality=DataQuality.METERED,
    )
    with pytest.raises(ValidationError):
        County(
            fips="2055",  # not 5 digits
            state="KS",
            name="Finney",
            aquifer_overlap_pct=0.92,
            data_quality=DataQuality.METERED,
        )


def test_aquifer_section_negative_decline_allowed():
    # Declining aquifers have negative annual_decline — must not error.
    AquiferSection(
        fips="20055",
        year=2023,
        saturated_thickness_m=22.4,
        annual_decline_m=-0.38,
        years_until_uneconomic=35.3,
    )


def test_crop_water_footprint_ok():
    CropWaterFootprint(
        fips="20055",
        crop="corn",
        year=2023,
        irrigated_acres=168_000,
        water_applied_acre_ft=220_000,
        irrigation_method_mix={"center_pivot": 0.9, "flood": 0.1},
        gross_value_usd=420_000_000,
    )


def test_custom_scenario_params_bounds():
    with pytest.raises(ValidationError):
        CustomScenarioParams(pumping_reduction_pct=1.5)


def test_scenario_result_shape():
    ScenarioResult(
        scenario_id=ScenarioID.KS_LEMA_AQUIFER_WIDE,
        run_id="abc123",
        computed_at=date(2026, 4, 18),
        aquifer_lifespan_extension_years=23.1,
        cumulative_ag_production_delta_usd_b=-14.2,
        rural_employment_delta_pct=-0.04,
        embedded_co2_delta_mt=-4_200_000,
        per_county=[
            CountyScenarioDelta(
                fips="20055",
                years_until_uneconomic_delta=23.0,
                ag_value_delta_usd=-12_000_000,
                employment_delta_fte=-18,
                co2_delta_mt=-5_400,
            )
        ],
    )
