"""Texas Water Development Board ingest.

Texas is the cautionary tale (spec §4): rule of capture → most counties lack
metered extraction. We pull well registrations + GCD boundaries + partial
reported pumpage; the gap gets closed by `analytics.extraction_imputation`.

Source: https://www.twdb.texas.gov/groundwater/data/
"""

from __future__ import annotations

from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)


def ingest_tx_wells() -> None:
    """Well registration database — lat/lon + capacity + use code."""
    raise NotImplementedError("Day 1 — teammate owns")


def ingest_tx_gcd_boundaries() -> None:
    """Groundwater Conservation District polygons; used in scenario engine."""
    raise NotImplementedError("Day 1 — teammate owns")
