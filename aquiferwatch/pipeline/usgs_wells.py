"""USGS High Plains Aquifer monitoring-well ingest.

Source: USGS Water Services (https://waterservices.usgs.gov/) + High Plains
Aquifer water-level network. No API key required; be polite with the user agent.

Target tables (on shared RDS, to be created via Alembic migration in parent project):
  - aqw_wells               (well_id, lat, lon, state, aquifer_code, county_fips)
  - aqw_well_measurements   (well_id, measurement_date, depth_to_water_ft, saturated_thickness_ft)

This module is a STUB: real implementation on Day 1 per spec §8.
"""

from __future__ import annotations

import argparse

from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)


def fetch_hpa_wells_smoke() -> int:
    """Minimal smoke test — hit the USGS site info endpoint for the HPA bounding box.

    Returns count of sites found. Used by `scripts/ingest_all.py --smoke`.
    """
    # TODO(day-1): implement against https://waterservices.usgs.gov/nwis/site/
    log.info("usgs_wells: smoke test — not yet implemented")
    return 0


def ingest_well_measurements(start_year: int = 1990) -> None:
    """Full historical pull of depth-to-water measurements for HPA wells."""
    # TODO(day-1): paginate by HUC-4 (1002, 1011, 1102, ...) inside the HPA footprint.
    raise NotImplementedError("Day 1 — teammate owns")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    if args.smoke:
        n = fetch_hpa_wells_smoke()
        log.info("Smoke result: %d sites", n)
