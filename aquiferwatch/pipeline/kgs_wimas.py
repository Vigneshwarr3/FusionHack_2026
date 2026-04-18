"""Kansas Geological Survey WIMAS ingest.

Kansas is our pilot state for rigor (spec §7 risk 1): best-instrumented, metered
extractions by well. Use it as ground truth for Texas imputation validation.

Source: https://www.kgs.ku.edu/Magellan/WaterWell/
Target table: aqw_kgs_wimas_pumpage (well_id, year, acre_feet, use_type, county_fips)
"""

from __future__ import annotations

from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)


def ingest_kgs_pumpage(year_start: int = 1990) -> None:
    """Pull KGS WIMAS annual pumpage. Kansas publishes a yearly CSV."""
    # TODO(day-1): WIMAS exports are CSV zip files; parse + upsert to RDS.
    raise NotImplementedError("Day 1 — teammate owns")
