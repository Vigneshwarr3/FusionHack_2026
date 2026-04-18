"""Economic and emissions overlays (spec §8, Day 5).

- $ per acre-foot per county per crop    (ERS crop budgets / water applied)
- Rural employment multipliers            (USDA ERS IMPLAN-style)
- Embedded CO2 of pumping electricity     (EIA state grid intensity × kWh/acre-ft)
"""

from __future__ import annotations

import pandas as pd


def dollar_per_acre_foot(crop_value_usd: pd.Series, water_applied_af: pd.Series) -> pd.Series:
    return crop_value_usd / water_applied_af.where(water_applied_af > 0)


def embedded_co2_mt(pumping_af: pd.Series, kwh_per_af: float, grid_intensity_kg_per_kwh: pd.Series):
    """Metric tons CO2 equivalent. grid_intensity_kg_per_kwh indexed by state."""
    # TODO(day-5): pull EIA grid intensity by state, per-year if available.
    raise NotImplementedError("Day 5 — paired")
