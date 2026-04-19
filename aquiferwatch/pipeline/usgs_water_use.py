"""USGS Estimated Use of Water 2015 — county-level withdrawals by category.

Independent cross-validation of our baseline's inferred pumping. USGS publishes
quinquennially; 2015 is the most recent released. Principal fields we use:
  IR-WGWFr  = Irrigation total self-supplied groundwater withdrawals, Mgal/day
  IR-CUsFr  = Irrigation consumptive use, Mgal/day (fresh)
  IC-WGWFr  = Irrigation, crop — groundwater withdrawals
  IT-WGWFr  = Irrigation, thermoelectric — not relevant

Source: ScienceBase item 5af3311be4b0da30c1b245d8 (usco2015v2.0.csv).

Output: data/processed/usgs_water_use_2015.parquet
Columns: fips, state, county_name, irrigation_gw_mgal_per_day,
         irrigation_gw_af_per_year (Mgal/day × 1120 = acre-ft/year)
"""

from __future__ import annotations

import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger, http_session

log = get_logger(__name__)

SCIENCEBASE_ITEM = "5af3311be4b0da30c1b245d8"
SCIENCEBASE_FILENAME = "usco2015v2.0.csv"
RAW = DATA_DIR / "raw" / "usgs_water_use" / SCIENCEBASE_FILENAME
OUTPUT = DATA_DIR / "processed" / "usgs_water_use_2015.parquet"

HPA_STATES = ("NE", "KS", "CO", "TX", "OK", "NM", "SD", "WY")

# 1 Mgal/day = 1,120.15 acre-feet/year (conversion constant)
MGAL_PER_DAY_TO_AF_PER_YR = 1120.15


def download() -> bytes:
    RAW.parent.mkdir(parents=True, exist_ok=True)
    if RAW.exists() and RAW.stat().st_size > 1000:
        return RAW.read_bytes()
    session = http_session("AquiferWatch-USGS-WU/0.1")
    # ScienceBase file URLs embed a disk-hash that rotates. Resolve via the
    # item's file list rather than hardcoding.
    meta = session.get(
        f"https://www.sciencebase.gov/catalog/item/{SCIENCEBASE_ITEM}",
        params={"format": "json"}, timeout=30,
    ).json()
    target = next(
        (f for f in meta.get("files", []) if f.get("name") == SCIENCEBASE_FILENAME),
        None,
    )
    if target is None:
        raise RuntimeError(f"file {SCIENCEBASE_FILENAME} not in ScienceBase item {SCIENCEBASE_ITEM}")
    log.info("  downloading %s (%d bytes)...", target["name"], target.get("size", 0))
    r = session.get(target["url"], timeout=120)
    r.raise_for_status()
    RAW.write_bytes(r.content)
    return r.content


def ingest() -> pd.DataFrame:
    download()
    # Row 0 is the citation; actual column headers live on row 1.
    df = pd.read_csv(RAW, low_memory=False, header=1)
    log.info("  parsed %d county rows × %d cols", len(df), len(df.columns))

    # FIPS: STATEFIPS + COUNTYFIPS
    df["fips"] = (
        df["STATEFIPS"].astype(int).astype(str).str.zfill(2)
        + df["COUNTYFIPS"].astype(int).astype(str).str.zfill(3)
    )

    # Irrigation groundwater withdrawals. "IR-WGWFr" = Irrigation total,
    # self-supplied fresh groundwater, Mgal/d. "IC-WGWFr" = Irrigation-crop
    # subset (excludes golf course). IC is closer to what we model.
    cols = {c for c in df.columns if c in ("IR-WGWFr", "IC-WGWFr", "IR-CUsFr", "IC-CUsFr")}
    if not cols:
        raise RuntimeError(
            "expected irrigation-withdrawal columns missing; "
            f"got columns like: {df.columns.tolist()[:20]}"
        )

    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    out_cols = {
        "fips": df["fips"],
        "state": df.get("STATE", pd.Series(index=df.index, dtype=object)),
        "county_name": df.get("COUNTY", pd.Series(index=df.index, dtype=object)),
    }
    if "IR-WGWFr" in df.columns:
        out_cols["irrigation_gw_mgal_per_day"] = df["IR-WGWFr"]
        out_cols["irrigation_gw_af_per_year"] = df["IR-WGWFr"] * MGAL_PER_DAY_TO_AF_PER_YR
    if "IC-WGWFr" in df.columns:
        out_cols["irrigation_crop_gw_mgal_per_day"] = df["IC-WGWFr"]
        out_cols["irrigation_crop_gw_af_per_year"] = df["IC-WGWFr"] * MGAL_PER_DAY_TO_AF_PER_YR
    if "IR-CUsFr" in df.columns:
        out_cols["irrigation_consumptive_mgal_per_day"] = df["IR-CUsFr"]
    out = pd.DataFrame(out_cols)

    # Filter to HPA states
    hpa = out[out["state"].isin(HPA_STATES)].copy()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    hpa.to_parquet(OUTPUT, index=False)
    log.info(
        "Wrote %d HPA-state county rows (total irrigation GW: %.0fk AF/yr in 2015) → %s",
        len(hpa), hpa["irrigation_gw_af_per_year"].sum() / 1000, OUTPUT,
    )
    return hpa


if __name__ == "__main__":
    ingest()
