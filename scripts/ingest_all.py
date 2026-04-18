"""Orchestrator — run the full pipeline end-to-end.

Day 0 version is a dispatcher skeleton. Each sub-step is implemented by its
owner (see docs/data_sources.md "Owner" column).
"""

from __future__ import annotations

import argparse

from aquiferwatch.pipeline import usgs_wells


def main(smoke: bool = False) -> None:
    if smoke:
        n = usgs_wells.fetch_hpa_wells_smoke()
        print(f"USGS smoke: {n} sites")
        return
    raise NotImplementedError(
        "Full ingest lands after Day 1 — see teammate's usgs_wells, kgs_wimas, tx_twdb, ne_dnr."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    main(smoke=args.smoke)
