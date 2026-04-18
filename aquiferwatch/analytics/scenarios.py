"""Scenario engine — the demo differentiator (spec §6).

Every scenario is a deterministic function of (baseline, params) → per-county
deltas + aquifer-level aggregates. No black boxes. Each formula below has a
1:1 reference in `docs/scenarios.md` so a journalist or policy analyst can
audit any number on screen.

Engine contract
---------------
    run_scenario(scenario_id, baseline_df, params=None) → ScenarioResult

`baseline_df` columns (strict — see `validate_baseline`):
    fips                       (5-digit FIPS)
    state                      (NE/KS/CO/TX/OK/NM/SD/WY)
    saturated_thickness_m
    annual_decline_m           (signed; negative = depleting)
    recharge_mm_yr
    pumping_af_yr              (current annual extraction)
    irrigated_acres_total
    ag_value_usd               (gross ag value)
    employment_fte             (rural ag employment, FTE)
    kwh_per_af_pumped          (pumping energy intensity)
    grid_intensity_kg_per_kwh  (state grid carbon intensity)
    acres_corn, acres_soybeans, acres_sorghum, acres_wheat, acres_cotton, acres_alfalfa
    irr_center_pivot, irr_flood, irr_drip, irr_dryland   (share of irrigated acres)

All scenarios share `_apply_and_summarize` so the delta math is consistent.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from aquiferwatch.schemas import (
    CountyScenarioDelta,
    CustomScenarioParams,
    ScenarioID,
    ScenarioResult,
)

# ---------------------------------------------------------------------------
# Constants — all cited in docs/scenarios.md
# ---------------------------------------------------------------------------
CROPS = ["corn", "soybeans", "sorghum", "wheat", "cotton", "alfalfa"]

# Water use (acre-ft per acre per year) — USDA IWMS 2023 average by crop.
WATER_PER_ACRE = {
    "corn": 1.40, "soybeans": 1.00, "sorghum": 0.80,
    "wheat": 0.70, "cotton": 1.10, "alfalfa": 1.80,
}
# Gross revenue per irrigated acre — USDA ERS county-level crop budgets, HPA median.
REVENUE_PER_ACRE = {
    "corn": 1350, "soybeans": 850, "sorghum": 560,
    "wheat": 420, "cotton": 980, "alfalfa": 1100,
}

# Irrigation method efficiency factor (share of water_per_acre actually needed).
# Drip/micro = 30–40% savings vs. center pivot baseline → use 0.65.
METHOD_EFFICIENCY = {
    "center_pivot": 1.00,
    "flood": 1.15,      # Flood is less efficient
    "drip": 0.65,
    "dryland": 0.0,     # No irrigation
}

# Uneconomic threshold — below 9 m saturated thickness, center-pivot pumping
# is uneconomic at typical Ogallala pumping depths (Deines 2019).
THICKNESS_THRESHOLD_M = 9.0

# Rural-employment multiplier: USDA ERS IMPLAN-style — ~0.021 FTE per $1000
# of gross ag value delta.
EMPLOYMENT_PER_AG_USD = 0.021 / 1000.0

BASELINE_YEAR_DEFAULT = 2024


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
REQUIRED_COLS = {
    "fips", "state",
    "saturated_thickness_m", "annual_decline_m", "recharge_mm_yr",
    "pumping_af_yr", "irrigated_acres_total",
    "ag_value_usd", "employment_fte",
    "kwh_per_af_pumped", "grid_intensity_kg_per_kwh",
    *(f"acres_{c}" for c in CROPS),
    "irr_center_pivot", "irr_flood", "irr_drip", "irr_dryland",
}


def validate_baseline(baseline: pd.DataFrame) -> None:
    missing = REQUIRED_COLS - set(baseline.columns)
    if missing:
        raise ValueError(f"baseline missing columns: {sorted(missing)}")
    if baseline["fips"].duplicated().any():
        raise ValueError("baseline has duplicate FIPS rows — pass one row per county")
    # Annual decline stored signed: typical HPA counties have negative decline.


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------
def run_scenario(
    scenario_id: ScenarioID,
    baseline: pd.DataFrame,
    params: CustomScenarioParams | None = None,
    *,
    run_id: str = "local",
) -> ScenarioResult:
    """Dispatch to the named scenario and return a full ScenarioResult."""
    validate_baseline(baseline)
    dispatcher = {
        ScenarioID.STATUS_QUO: _status_quo,
        ScenarioID.KS_LEMA_AQUIFER_WIDE: _ks_lema_aquifer_wide,
        ScenarioID.DRIP_TRANSITION: _drip_transition,
        ScenarioID.CORN_REDUCTION_25: _corn_reduction_25,
        ScenarioID.NO_AG_BELOW_9M: _no_ag_below_9m,
        ScenarioID.CUSTOM: _custom,
    }
    modified = dispatcher[scenario_id](baseline.copy(), params)
    return _apply_and_summarize(baseline, modified, scenario_id, run_id)


# ---------------------------------------------------------------------------
# Scenarios
#
# Each one returns a MODIFIED copy of the baseline. `_apply_and_summarize`
# then diffs them to produce the deltas. Keep each scenario small and
# readable — they are the audit surface.
# ---------------------------------------------------------------------------
def _status_quo(b: pd.DataFrame, params) -> pd.DataFrame:
    # Identity — baseline unchanged. Serves as the comparison anchor.
    return b


def _ks_lema_aquifer_wide(b: pd.DataFrame, params) -> pd.DataFrame:
    """Apply Sheridan-6 LEMA rules to every HPA county.

    Per Basso et al. 2025: ~27% pumping reduction on average, crop mix shifts
    away from corn (-15% of corn acres) toward sorghum (+60% of the shift) and
    wheat (+40% of the shift). Rural employment impact absorbed via
    downstream delta engine.
    """
    b = _shift_corn_to(b, fraction=0.15, targets={"sorghum": 0.6, "wheat": 0.4})
    b["pumping_af_yr"] = b["pumping_af_yr"] * (1 - 0.27)
    return b


def _drip_transition(b: pd.DataFrame, params) -> pd.DataFrame:
    """10-year transition of center-pivot + flood acres to drip.

    We model the endstate (all flood + center-pivot → drip). Water savings
    come through METHOD_EFFICIENCY: the new method mix reduces effective
    water applied, which we translate to a proportional pumping reduction.
    """
    baseline_eff = _effective_efficiency(b)
    b["irr_drip"] = b["irr_drip"] + b["irr_center_pivot"] + b["irr_flood"]
    b["irr_center_pivot"] = 0.0
    b["irr_flood"] = 0.0
    new_eff = _effective_efficiency(b)
    ratio = new_eff / baseline_eff.replace(0, 1)  # avoid /0 for pure-dryland
    b["pumping_af_yr"] = b["pumping_af_yr"] * ratio
    return b


def _corn_reduction_25(b: pd.DataFrame, params) -> pd.DataFrame:
    """Corn acreage drops 25%; reallocated 50% sorghum, 30% wheat, 20% dryland.

    Dryland share becomes un-irrigated — remove from `irrigated_acres_total`
    proportionally; update pumping from the new crop mix.
    """
    shifted = _shift_corn_to(
        b, fraction=0.25,
        targets={"sorghum": 0.50, "wheat": 0.30, "__dryland__": 0.20},
    )
    shifted["pumping_af_yr"] = _pumping_from_crop_mix(shifted)
    return shifted


def _no_ag_below_9m(b: pd.DataFrame, params) -> pd.DataFrame:
    """Counties below 9 m saturated thickness stop pumping. Acres go dryland."""
    past = b["saturated_thickness_m"] < THICKNESS_THRESHOLD_M
    for c in CROPS:
        b.loc[past, f"acres_{c}"] = 0.0
    b.loc[past, "irrigated_acres_total"] = 0.0
    b.loc[past, "pumping_af_yr"] = 0.0
    b.loc[past, "ag_value_usd"] = 0.0
    b.loc[past, "irr_dryland"] = 1.0
    b.loc[past, ["irr_center_pivot", "irr_flood", "irr_drip"]] = 0.0
    return b


def _custom(b: pd.DataFrame, params: CustomScenarioParams | None) -> pd.DataFrame:
    if params is None:
        raise ValueError("CUSTOM scenario requires CustomScenarioParams")
    # Stack the three levers, each reusing its built-in scenario's math at
    # a fractional intensity.
    if params.corn_to_sorghum_shift_pct > 0:
        b = _shift_corn_to(
            b, fraction=params.corn_to_sorghum_shift_pct,
            targets={"sorghum": 1.0},
        )
    if params.drip_adoption_pct > 0:
        baseline_eff = _effective_efficiency(b)
        # Move `drip_adoption_pct` of center-pivot+flood to drip.
        shift = (b["irr_center_pivot"] + b["irr_flood"]) * params.drip_adoption_pct
        b["irr_drip"] = b["irr_drip"] + shift
        cp_share = b["irr_center_pivot"] / (b["irr_center_pivot"] + b["irr_flood"]).replace(0, 1)
        b["irr_center_pivot"] = b["irr_center_pivot"] - shift * cp_share
        b["irr_flood"] = b["irr_flood"] - shift * (1 - cp_share)
        ratio = _effective_efficiency(b) / baseline_eff.replace(0, 1)
        b["pumping_af_yr"] = b["pumping_af_yr"] * ratio
    if params.pumping_reduction_pct > 0:
        b["pumping_af_yr"] = b["pumping_af_yr"] * (1 - params.pumping_reduction_pct)
    return b


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _shift_corn_to(
    b: pd.DataFrame, fraction: float, targets: dict[str, float]
) -> pd.DataFrame:
    """Move `fraction` of corn acres to other crops / dryland per `targets`.

    `targets` must sum to 1.0. Key "__dryland__" means remove from irrigated
    acres entirely (crop goes to dryland — no pumping, no ag value from that
    slice). Revenue and water recompute from the new crop mix.
    """
    assert abs(sum(targets.values()) - 1.0) < 1e-6, "targets must sum to 1.0"
    moved = b["acres_corn"] * fraction
    b["acres_corn"] = b["acres_corn"] - moved
    for tgt, share in targets.items():
        if tgt == "__dryland__":
            b["irrigated_acres_total"] = b["irrigated_acres_total"] - moved * share
            continue
        b[f"acres_{tgt}"] = b[f"acres_{tgt}"] + moved * share

    # Re-derive irrigated_acres_total from the crop acre sum (keeps everything
    # consistent).
    b["irrigated_acres_total"] = sum(b[f"acres_{c}"] for c in CROPS)
    b["ag_value_usd"] = _ag_value_from_crops(b)
    b["pumping_af_yr"] = _pumping_from_crop_mix(b)
    return b


def _effective_efficiency(b: pd.DataFrame) -> pd.Series:
    """Weighted avg of METHOD_EFFICIENCY across the irrigation method mix."""
    return (
        b["irr_center_pivot"] * METHOD_EFFICIENCY["center_pivot"]
        + b["irr_flood"] * METHOD_EFFICIENCY["flood"]
        + b["irr_drip"] * METHOD_EFFICIENCY["drip"]
        + b["irr_dryland"] * METHOD_EFFICIENCY["dryland"]
    )


def _pumping_from_crop_mix(b: pd.DataFrame) -> pd.Series:
    """Compute pumping = Σ_crop (acres × water_per_acre) × irrigation_efficiency."""
    base_water = sum(b[f"acres_{c}"] * WATER_PER_ACRE[c] for c in CROPS)
    return base_water * _effective_efficiency(b)


def _ag_value_from_crops(b: pd.DataFrame) -> pd.Series:
    return sum(b[f"acres_{c}"] * REVENUE_PER_ACRE[c] for c in CROPS)


# ---------------------------------------------------------------------------
# Delta engine — the math every scenario produces identically
# ---------------------------------------------------------------------------
def _apply_and_summarize(
    base: pd.DataFrame,
    mod: pd.DataFrame,
    scenario_id: ScenarioID,
    run_id: str,
) -> ScenarioResult:
    """Diff baseline vs. modified → per-county deltas + aquifer-level summary."""
    joined = base[["fips", "saturated_thickness_m", "annual_decline_m",
                   "recharge_mm_yr", "pumping_af_yr", "ag_value_usd",
                   "employment_fte", "kwh_per_af_pumped",
                   "grid_intensity_kg_per_kwh"]].merge(
        mod[["fips", "pumping_af_yr", "ag_value_usd", "employment_fte"]],
        on="fips", suffixes=("_base", "_mod"),
    )

    # Recharge (mm/yr) → meters over the county area isn't modeled here; we
    # use `recharge_mm_yr / 1000` as a proxy thickness recharge per year.
    # New annual_decline = old_decline scaled by the pumping ratio.
    # old_decline already accounts for old pumping, so:
    pump_ratio = joined["pumping_af_yr_mod"] / joined["pumping_af_yr_base"].replace(0, 1)
    new_decline = joined["annual_decline_m"] * pump_ratio
    # Years until uneconomic under baseline
    yrs_base = _years_until_uneconomic(
        joined["saturated_thickness_m"], joined["annual_decline_m"]
    )
    yrs_mod = _years_until_uneconomic(joined["saturated_thickness_m"], new_decline)
    yrs_delta = yrs_mod - yrs_base

    ag_delta = joined["ag_value_usd_mod"] - joined["ag_value_usd_base"]
    emp_delta = EMPLOYMENT_PER_AG_USD * ag_delta  # IMPLAN-style
    pump_delta_af = joined["pumping_af_yr_mod"] - joined["pumping_af_yr_base"]
    co2_delta_mt = (
        pump_delta_af
        * joined["kwh_per_af_pumped"]
        * joined["grid_intensity_kg_per_kwh"]
        / 1000.0
    )

    per_county = [
        CountyScenarioDelta(
            fips=str(row["fips"]),
            years_until_uneconomic_delta=float(_safe(yrs_delta.iloc[i])),
            ag_value_delta_usd=float(ag_delta.iloc[i]),
            employment_delta_fte=float(emp_delta.iloc[i]),
            co2_delta_mt=float(co2_delta_mt.iloc[i]),
        )
        for i, row in joined.iterrows()
    ]

    # Aquifer-level aggregates.
    # Lifespan extension: area-weighted mean years-delta over HPA counties
    # (weight = current pumping, since a county pumping more matters more).
    weights = joined["pumping_af_yr_base"].clip(lower=0)
    if weights.sum() > 0:
        lifespan_ext = float((yrs_delta * weights).sum() / weights.sum())
    else:
        lifespan_ext = 0.0

    total_ag_delta_usd_b = float(ag_delta.sum()) / 1e9
    baseline_total_emp = joined["employment_fte_base"].sum()
    rural_emp_delta_pct = (
        float(emp_delta.sum() / baseline_total_emp) if baseline_total_emp else 0.0
    )
    co2_total_mt = float(co2_delta_mt.sum())

    return ScenarioResult(
        scenario_id=scenario_id,
        run_id=run_id,
        computed_at=date.today(),
        aquifer_lifespan_extension_years=lifespan_ext,
        cumulative_ag_production_delta_usd_b=total_ag_delta_usd_b,
        rural_employment_delta_pct=rural_emp_delta_pct,
        embedded_co2_delta_mt=co2_total_mt,
        per_county=per_county,
    )


def _years_until_uneconomic(thickness_m: pd.Series, decline_m_yr: pd.Series) -> pd.Series:
    """Years until thickness hits the 9 m threshold.

    Schema convention (matches aquiferwatch.schemas.AquiferSection):
        decline_m_yr < 0  → aquifer depleting (typical HPA)
        decline_m_yr ≥ 0  → stable or rising → infinite lifespan (capped at 1000)

    A county already below the threshold has 0 years remaining regardless of direction.
    """
    headroom = (thickness_m - THICKNESS_THRESHOLD_M).clip(lower=0)
    # Only decline < 0 is depleting. Non-negative decline → infinite lifespan.
    depleting = decline_m_yr < 0
    depletion_rate = (-decline_m_yr).where(depleting, other=1e-9)
    yrs = headroom / depletion_rate
    return yrs.clip(upper=1000.0)


def _safe(x) -> float:
    try:
        v = float(x)
    except Exception:
        return 0.0
    if v != v or v in (float("inf"), float("-inf")):
        return 0.0
    return v
