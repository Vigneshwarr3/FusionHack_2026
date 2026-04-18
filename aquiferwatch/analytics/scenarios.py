"""Scenario engine — the differentiator (spec §6).

Every scenario is a deterministic function of (baseline state, scenario params)
→ per-county deltas. No black boxes. Each scenario's math must be traceable to
a specific section in docs/scenarios.md.

Unit-tested under tests/test_scenarios.py. Exposed via api/router.py.
"""

from __future__ import annotations

import pandas as pd

from aquiferwatch.schemas import (
    CustomScenarioParams,
    ScenarioID,
    ScenarioResult,
)


def run_scenario(
    scenario_id: ScenarioID,
    baseline: pd.DataFrame,
    params: CustomScenarioParams | None = None,
) -> ScenarioResult:
    """Dispatch to the named scenario implementation.

    Parameters
    ----------
    baseline : county-year frame with columns
               [fips, saturated_thickness_m, pumping_af_yr, crop_mix, ag_value_usd, ...]
    params   : required only for ScenarioID.CUSTOM
    """
    dispatcher = {
        ScenarioID.STATUS_QUO: _status_quo,
        ScenarioID.KS_LEMA_AQUIFER_WIDE: _ks_lema_aquifer_wide,
        ScenarioID.DRIP_TRANSITION: _drip_transition,
        ScenarioID.CORN_REDUCTION_25: _corn_reduction_25,
        ScenarioID.NO_AG_BELOW_9M: _no_ag_below_9m,
        ScenarioID.CUSTOM: _custom,
    }
    impl = dispatcher[scenario_id]
    return impl(baseline, params)


def _status_quo(baseline, params):
    raise NotImplementedError("Day 4 — teammate owns")


def _ks_lema_aquifer_wide(baseline, params):
    # Source: Basso et al. 2025 — ~25-30% pumping reduction + sorghum/wheat shift
    raise NotImplementedError("Day 4 — teammate owns")


def _drip_transition(baseline, params):
    # Apply 30-40% water savings to center-pivot + flood acres over 10 years
    raise NotImplementedError("Day 4 — teammate owns")


def _corn_reduction_25(baseline, params):
    # 25% corn → 50% sorghum / 30% wheat / 20% dryland
    raise NotImplementedError("Day 4 — teammate owns")


def _no_ag_below_9m(baseline, params):
    # Counties below 9m saturated thickness stop pumping entirely
    raise NotImplementedError("Day 4 — teammate owns")


def _custom(baseline, params):
    if params is None:
        raise ValueError("CUSTOM scenario requires CustomScenarioParams")
    raise NotImplementedError("Day 4 — Raj owns (UI slider integration)")
