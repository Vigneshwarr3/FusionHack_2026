"""Shared Pydantic contracts. Locked on Day 0 per spec §7.

These are the interfaces that both the pipeline/analytics side and the API/frontend side
must agree on. Breaking changes require a PR review.
"""

from __future__ import annotations

from datetime import date
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


STATE_CODES = Literal["NE", "KS", "CO", "TX", "OK", "NM", "SD", "WY"]
CROPS = Literal["corn", "soybeans", "sorghum", "wheat", "cotton", "alfalfa"]
IRRIGATION_METHODS = Literal["center_pivot", "flood", "drip", "dryland"]


class DataQuality(str, Enum):
    """Per-county data quality flag. Shown as an overlay on the map (spec §4)."""

    METERED = "metered"  # KS, NE parts — direct extraction measurement
    MODELED_HIGH = "modeled_high"  # Good well density, reliable imputation
    MODELED_LOW = "modeled_low"  # Sparse monitoring (much of TX, WY, SD)
    NO_DATA = "no_data"


class County(BaseModel):
    fips: str = Field(pattern=r"^\d{5}$")
    state: STATE_CODES
    name: str
    aquifer_overlap_pct: float = Field(ge=0, le=1, description="Fraction of county over HPA")
    data_quality: DataQuality


class AquiferSection(BaseModel):
    """Saturated-thickness snapshot for a county at a point in time."""

    fips: str
    year: int
    saturated_thickness_m: float = Field(ge=0)
    annual_decline_m: float = Field(description="Signed: negative means depleting")
    years_until_uneconomic: float | None = Field(
        default=None,
        description="Projected years until <9m saturated thickness; None if already past.",
    )
    recharge_rate_mm_yr: float | None = None


class CropWaterFootprint(BaseModel):
    """Per-county, per-crop, per-year water use and economic return."""

    fips: str
    crop: CROPS
    year: int
    irrigated_acres: float = Field(ge=0)
    water_applied_acre_ft: float = Field(ge=0)
    irrigation_method_mix: dict[IRRIGATION_METHODS, float] = Field(
        default_factory=dict,
        description="Shares sum to ~1.0",
    )
    gross_value_usd: float = Field(ge=0)
    dollar_per_acre_ft: float | None = None


class ScenarioID(str, Enum):
    STATUS_QUO = "status_quo"
    KS_LEMA_AQUIFER_WIDE = "ks_lema_aquifer_wide"
    DRIP_TRANSITION = "drip_transition"
    CORN_REDUCTION_25 = "corn_reduction_25"
    NO_AG_BELOW_9M = "no_ag_below_9m"
    CUSTOM = "custom"


class CustomScenarioParams(BaseModel):
    pumping_reduction_pct: float = Field(default=0.0, ge=0, le=1)
    corn_to_sorghum_shift_pct: float = Field(default=0.0, ge=0, le=1)
    drip_adoption_pct: float = Field(default=0.0, ge=0, le=1)


class Scenario(BaseModel):
    id: ScenarioID
    display_name: str
    description: str
    params: CustomScenarioParams | None = None
    source: str | None = Field(
        default=None,
        description="Citation for the scenario's assumptions (e.g., Basso et al. 2025).",
    )


class CountyScenarioDelta(BaseModel):
    fips: str
    years_until_uneconomic_delta: float
    ag_value_delta_usd: float
    employment_delta_fte: float
    co2_delta_mt: float


class ScenarioResult(BaseModel):
    scenario_id: ScenarioID
    run_id: str = Field(description="MLflow run_id for traceability")
    computed_at: date
    aquifer_lifespan_extension_years: float
    cumulative_ag_production_delta_usd_b: float
    rural_employment_delta_pct: float
    embedded_co2_delta_mt: float
    per_county: list[CountyScenarioDelta]
