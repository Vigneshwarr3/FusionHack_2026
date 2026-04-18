"""Nebraska DNR ingest.

Nebraska is the positive story (spec §9 featured story 3): best-instrumented
non-Kansas state, strong NRD conservation districts, slowest depletion.

Source: https://dnr.nebraska.gov/  (registered wells, NRD use reports)
"""

from __future__ import annotations

from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)


def ingest_ne_wells() -> None:
    raise NotImplementedError("Day 1 — teammate owns")


def ingest_ne_nrd_use_reports() -> None:
    """Natural Resource District annual groundwater use summaries."""
    raise NotImplementedError("Day 1 — teammate owns")
