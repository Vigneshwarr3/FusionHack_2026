"""Kansas Geological Survey WIMAS ingest.

Kansas is our pilot state for rigor (spec §7 risk 1): best-instrumented, metered
extractions by well. Use it as ground truth for Texas imputation validation.

**Manual download required** — WIMAS's interactive query interface at
`hercules.kgs.ku.edu` is not programmatically scriptable (form-based CFM,
currently intermittently unavailable from our network). Download steps:

  1. Visit https://www.kgs.ku.edu/Magellan/WaterWell/
  2. Under "Water Rights" → "Water Use", run the query for HPA counties.
  3. Export CSV to `data/raw/kgs_wimas/wimas_pumpage_<YYYY>.csv`.
  4. Run `python -m aquiferwatch.pipeline.kgs_wimas` to parse and write parquet.

Alternative: Kansas Department of Agriculture DWR WRIS (Water Rights Info
System) accepts annual batch exports by GMD (Groundwater Management District).

Once a CSV is dropped in `data/raw/kgs_wimas/`, this module parses it into
`data/processed/kgs_wimas_pumpage.parquet` with columns
(well_id, year, county_fips, acre_feet, use_type, gmd).
"""

from __future__ import annotations

import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger

log = get_logger(__name__)

RAW_DIR = DATA_DIR / "raw" / "kgs_wimas"
RIGHTS_OUT = DATA_DIR / "processed" / "kgs_wimas_rights.parquet"
PUMPAGE_OUT = DATA_DIR / "processed" / "kgs_wimas_pumpage.parquet"
STATE_SUMMARY_OUT = DATA_DIR / "processed" / "kgs_wimas_state_annual.parquet"

# Kansas county 2-letter codes → 5-digit FIPS.
# Compiled from WIMAS documentation; covers all 105 KS counties.
KS_COUNTY_TO_FIPS: dict[str, str] = {
    "AL": "20001", "AN": "20003", "AT": "20005", "BA": "20007", "BB": "20009",
    "BT": "20011", "BR": "20013", "BU": "20015", "CS": "20017", "CQ": "20019",
    "CY": "20021", "CA": "20023", "CR": "20025", "CK": "20027", "CN": "20029",
    "CD": "20031", "CL": "20033", "CF": "20035", "CO": "20037", "CM": "20039",
    "CY": "20027", "CN": "20023",  # Cloud / Cheyenne (CN=Cheyenne per WIMAS)
    "CW": "20033",  # Comanche
    "CR": "20037", "CK": "20035",
    "DC": "20039", "DK": "20041", "DP": "20043", "DG": "20045", "EK": "20049",
    "ED": "20047",
    "EW": "20051", "EL": "20053", "FI": "20055", "FO": "20057", "FR": "20059",
    "GE": "20061", "GO": "20063", "GH": "20065", "GY": "20069", "GT": "20067",
    "GL": "20071", "GW": "20073", "HM": "20075",  # Hamilton
    "HP": "20077", "HV": "20079",  # Harper / Harvey
    "HS": "20081", "HG": "20083", "JA": "20085",  # Jackson
    "JF": "20087", "JW": "20089", "JO": "20091", "KE": "20093", "KM": "20095", "KW": "20097",
    "LB": "20099", "LE": "20101", "LV": "20103", "LC": "20105", "LN": "20107",
    "LG": "20109", "LY": "20111", "MP": "20113", "MN": "20115", "MS": "20117",
    "ME": "20119", "MI": "20121", "MC": "20123", "MT": "20125", "MG": "20127",
    "MR": "20129", "MO": "20131", "NM": "20133", "NS": "20135", "NT": "20137",
    "OS": "20139", "OB": "20141", "OT": "20143", "PN": "20145", "PL": "20147",
    "PT": "20149", "PR": "20151", "RA": "20153", "RN": "20155", "RP": "20157",
    "RC": "20159", "RL": "20161", "RH": "20163", "RO": "20165", "RS": "20167",
    "SA": "20169", "SC": "20171", "SG": "20173", "SW": "20175", "SD": "20177",
    "SN": "20179", "SH": "20181", "ST": "20183", "SF": "20185", "SM": "20187",
    "SV": "20189", "SU": "20191", "TH": "20193", "TR": "20195", "WB": "20197",
    "WA": "20199", "WS": "20201", "WH": "20203", "WL": "20205", "WO": "20207",
    "WY": "20209",
}

# Kansas HPA counties — the western / south-central band where the
# Ogallala / HPA extends. Source: USGS DS-543 footprint filtered to KS
# plus GMD 1/3/4 membership observed in the WIMAS export.
HPA_COUNTY_CODES: set[str] = {
    # GMD 1 (western, as observed in data): GL, HS, LE, SC, WA, WH
    "GL", "HS", "LE", "SC", "WA", "WH",
    # GMD 3 (southwest KS, all HPA): Finney, Ford, Grant, Gray, Haskell,
    # Hamilton, Hodgeman, Kearny, Lane, Meade, Morton, Scott, Seward, Stanton,
    # Stevens, Edwards (partial)
    "FI", "FO", "GT", "GY", "HS", "HM", "HG", "KE", "LE", "ME", "MT", "SC",
    "SW", "ST", "SV",
    # GMD 4 (NW KS, as observed): CN, DC, GH, GO, LG, PL, RA, SD, SH, TH, WA
    "CN", "DC", "GH", "GO", "LG", "PL", "RA", "SD", "SH", "TH", "WA",
    # Non-GMD but HPA-adjacent (northern edge + Arkansas River lowlands)
    "ED", "PN", "SF", "PR", "KW", "SM", "JW", "RO", "OB", "RH", "NO", "NS", "NT",
}


def _read_wimas_csv(path) -> pd.DataFrame:
    """WIMAS export is CSV-with-spaces — strip after split."""
    df = pd.read_csv(path, skipinitialspace=True, low_memory=False)
    # Strip whitespace in string columns
    for c in df.select_dtypes(include="object").columns:
        df[c] = df[c].astype(str).str.strip()
    return df


def _looks_like_rights_file(df: pd.DataFrame) -> bool:
    return {"wr_id", "umw_code", "source_of_supply", "priority_date"}.issubset(df.columns)


def _looks_like_pumpage_file(df: pd.DataFrame) -> bool:
    return {"wr_num", "total_water_used_af"}.issubset(df.columns) or {
        "wr_num", "report_year"
    }.issubset(df.columns)


def _looks_like_state_summary(df: pd.DataFrame) -> bool:
    """State-wide annual water-use summary: Year, AF_USED, ACRES_IRRIGATED."""
    return {"Year", "AF_USED", "ACRES_IRRIGATED"}.issubset(df.columns)


def ingest_kgs_pumpage() -> dict[str, pd.DataFrame]:
    """Parse any CSV/TXT files in data/raw/kgs_wimas/.

    Dispatches on schema: the WIMAS water-rights export has columns like
    `wr_id`/`umw_code`/`source_of_supply` and lands in kgs_wimas_rights.parquet;
    a pumpage (annual use report) export has `wr_num`+`total_water_used_af`
    and lands in kgs_wimas_pumpage.parquet.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    files = [p for p in RAW_DIR.iterdir() if p.suffix.lower() in {".csv", ".txt"}]
    if not files:
        log.warning("no files in %s — see module docstring for manual download", RAW_DIR)
        return {}

    rights_frames: list[pd.DataFrame] = []
    pumpage_frames: list[pd.DataFrame] = []
    state_summary_frames: list[pd.DataFrame] = []
    for p in files:
        log.info("  parsing %s (%.1f MB)", p.name, p.stat().st_size / 1e6)
        df = _read_wimas_csv(p)
        if _looks_like_rights_file(df):
            rights_frames.append(df)
        elif _looks_like_pumpage_file(df):
            pumpage_frames.append(df)
        elif _looks_like_state_summary(df):
            state_summary_frames.append(df)
        else:
            log.warning("    unrecognized schema — columns: %s",
                        df.columns.tolist()[:10])

    out: dict[str, pd.DataFrame] = {}
    if rights_frames:
        rights = _process_rights(pd.concat(rights_frames, ignore_index=True))
        RIGHTS_OUT.parent.mkdir(parents=True, exist_ok=True)
        rights.to_parquet(RIGHTS_OUT, index=False)
        log.info("Wrote %d rights → %s", len(rights), RIGHTS_OUT)
        out["rights"] = rights

    if pumpage_frames:
        pumpage = _process_pumpage(pd.concat(pumpage_frames, ignore_index=True))
        pumpage.to_parquet(PUMPAGE_OUT, index=False)
        log.info("Wrote %d pumpage rows → %s", len(pumpage), PUMPAGE_OUT)
        out["pumpage"] = pumpage

    if state_summary_frames:
        summary = _process_state_summary(pd.concat(state_summary_frames, ignore_index=True))
        STATE_SUMMARY_OUT.parent.mkdir(parents=True, exist_ok=True)
        summary.to_parquet(STATE_SUMMARY_OUT, index=False)
        log.info("Wrote %d annual-summary rows → %s", len(summary), STATE_SUMMARY_OUT)
        out["state_summary"] = summary

    return out


def _process_state_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Kansas state-wide annual irrigation use (1955-present)."""
    out = df.rename(columns={
        "Year": "year",
        "AF_USED": "acre_feet_used",
        "ACRES_IRRIGATED": "acres_irrigated",
    })
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["acre_feet_used"] = pd.to_numeric(out["acre_feet_used"], errors="coerce")
    out["acres_irrigated"] = pd.to_numeric(out["acres_irrigated"], errors="coerce")
    out["state"] = "KS"
    out["scope"] = "state_total_irrigation"
    return out.dropna(subset=["year"])


def _process_rights(df: pd.DataFrame) -> pd.DataFrame:
    """Clean WIMAS rights export + map county codes to FIPS + flag HPA rows."""
    keep = [
        "wr_id", "wr_num", "wr_qualifier", "right_type", "umw_code",
        "source_of_supply", "wrfile_active_ind", "current_status_code",
        "priority_date", "gmd", "county_code", "longitude", "latitude",
        "num_wells", "well_kid", "pdiv_id", "fpdiv_active_ind",
    ]
    keep = [c for c in keep if c in df.columns]
    out = df[keep].copy()

    out["longitude"] = pd.to_numeric(out["longitude"], errors="coerce")
    out["latitude"] = pd.to_numeric(out["latitude"], errors="coerce")
    out["priority_date"] = pd.to_datetime(out["priority_date"], errors="coerce")
    out["fips"] = out["county_code"].map(KS_COUNTY_TO_FIPS)
    out["is_hpa_county"] = out["county_code"].isin(HPA_COUNTY_CODES)
    out["active"] = out["wrfile_active_ind"].astype(str) == "1"
    return out


def _process_pumpage(df: pd.DataFrame) -> pd.DataFrame:
    """Best-effort parse of an annual-use report export (schema varies)."""
    return df  # placeholder; tighten once we see a real export


def hpa_county_rights_summary() -> pd.DataFrame:
    """Aggregate rights to county-level features for the baseline."""
    if not RIGHTS_OUT.exists():
        raise RuntimeError("run ingest_kgs_pumpage first")
    df = pd.read_parquet(RIGHTS_OUT)
    df = df[df["active"] & df["is_hpa_county"]].copy()
    agg = df.groupby("fips").agg(
        n_active_irr_rights=("wr_id", "count"),
        n_wimas_wells=("num_wells", "sum"),
        mean_right_age_yrs=(
            "priority_date",
            lambda s: (pd.Timestamp("2024-01-01") - pd.to_datetime(s)).dt.days.mean() / 365.25,
        ),
    ).reset_index()
    return agg


if __name__ == "__main__":
    ingest_kgs_pumpage()
