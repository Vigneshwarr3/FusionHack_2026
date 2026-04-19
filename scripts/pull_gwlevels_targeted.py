"""Targeted USGS gwlevels pull for HPA wells.

Draws ~600 KS wells from the KGS Master Inventory WIZARD_USGS_ID column and
~700 other-state wells from the USGS HPA wells parquet. Writes the combined
water-level time series to data/processed/usgs_gwlevels.parquet so downstream
joins (KGS Master Inventory → saturated thickness by KS county, TWDB → TX
thickness, etc.) can proceed.

Separate script (not an inline one-liner) so failures leave a visible
Python traceback in the task output file.
"""

from __future__ import annotations

import random

import pandas as pd

from aquiferwatch.pipeline.usgs_wells import fetch_gwlevels


def main() -> None:
    master = pd.read_parquet("data/processed/kgs_master_wells.parquet")
    ks_pool = (
        master[master["is_hpa_county"] & master["has_wizard_usgs_id"]]
        ["WIZARD_USGS_ID"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .unique()
        .tolist()
    )
    random.seed(42)
    random.shuffle(ks_pool)
    ks_sites = ks_pool[:600]

    usgs = pd.read_parquet("data/processed/usgs_wells.parquet")
    other_sites = (
        usgs[usgs["state_abbr"] != "KS"]
        .groupby("state_abbr", group_keys=False)
        .head(100)["site_no"]
        .astype(str)
        .unique()
        .tolist()
    )

    sites = ks_sites + other_sites
    print(f"Pulling gwlevels for {len(ks_sites)} KS + {len(other_sites)} other = {len(sites)} total")
    df = fetch_gwlevels(sites, datetime_range="2005-01-01/..", sleep_per_req=0.2)
    print(f"Rows returned: {len(df):,}")
    if len(df):
        df.to_parquet("data/processed/usgs_gwlevels.parquet", index=False)
        print(f'Unique sites with data: {df["site_no"].nunique():,}')
        print(f'Date range: {df["time"].min()} .. {df["time"].max()}')


if __name__ == "__main__":
    main()
