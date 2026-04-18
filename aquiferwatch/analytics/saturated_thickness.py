"""Saturated-thickness interpolation from point measurements to county polygons.

Monitoring-well density is heterogeneous (dense in Kansas/Nebraska, sparse in
Wyoming/SD/parts of Texas). We interpolate well-level depth-to-water to a
county-level saturated-thickness surface, then aggregate to county polygons.

Method (to be finalized Day 1): IDW baseline, kriging stretch goal. Validate
against Deines et al. 2019 Kansas results.
"""

from __future__ import annotations

import pandas as pd


def interpolate_to_counties(
    well_measurements: pd.DataFrame,
    county_polygons: pd.DataFrame,
    year: int,
) -> pd.DataFrame:
    """Return a per-county saturated thickness estimate for `year`.

    Parameters
    ----------
    well_measurements : columns [well_id, lat, lon, date, saturated_thickness_ft]
    county_polygons  : columns [fips, geometry]
    year             : snapshot year
    """
    raise NotImplementedError("Day 1 — teammate owns")
