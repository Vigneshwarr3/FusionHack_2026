"""Depletion projection per county.

Short horizon: linear regression on per-county saturated-thickness time series.
Long horizon: Deines-style mass balance (thickness + recharge - pumping → 9m threshold).
"""

from __future__ import annotations

import pandas as pd

from aquiferwatch.schemas import AquiferSection


def project_county(
    history: pd.DataFrame,
    recharge_mm_yr: float,
    pumping_rate_af_yr: float,
    horizon_years: int = 100,
) -> list[AquiferSection]:
    """Project annual saturated thickness forward for one county.

    Parameters
    ----------
    history          : columns [year, saturated_thickness_m] — at least 10 years
    recharge_mm_yr   : long-term average recharge rate
    pumping_rate_af_yr : current extraction rate
    horizon_years    : default 100 → 2125 out from today

    Returns
    -------
    List of AquiferSection, one per projected year.
    """
    raise NotImplementedError("Day 3 — teammate owns")
