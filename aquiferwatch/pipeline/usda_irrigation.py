"""USDA Irrigation and Water Management Survey ingest.

Gives per-crop water-application rates by state/region (2023 release, Nov 2024).
This is REUSE territory — the parent project has QuickStats patterns in
Agricultural_Data_Analysis/pipeline/quickstats_ingest.py that we mimic here.
"""

from __future__ import annotations

from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)


def ingest_irrigation_survey() -> None:
    """Per-crop acre-inches-applied from the 2023 IWMS."""
    # TODO(day-2): port the QuickStats request pattern from the parent repo.
    # Keep state partitioning + batch logic identical for consistency.
    raise NotImplementedError("Day 2 — paired (Raj + teammate)")
