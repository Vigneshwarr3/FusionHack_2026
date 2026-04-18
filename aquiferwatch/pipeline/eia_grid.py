"""EIA eGRID — state-level grid carbon intensity.

Source: EPA eGRID 2022 Excel file (released Jan 2024).
Output CO2 emission rate (lb/MWh) per state; converted to kg/kWh for the
scenario engine's pumping-emissions formula.

Output: data/processed/eia_grid_intensity.parquet
Columns: state, co2_kg_per_kwh, year
"""

from __future__ import annotations

from io import BytesIO

import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger, http_session

log = get_logger(__name__)

EGRID_URL = "https://www.epa.gov/system/files/documents/2024-01/egrid2022_data.xlsx"
RAW = DATA_DIR / "raw" / "eia" / "egrid2022_data.xlsx"
OUTPUT = DATA_DIR / "processed" / "eia_grid_intensity.parquet"


def download() -> bytes:
    RAW.parent.mkdir(parents=True, exist_ok=True)
    if RAW.exists() and RAW.stat().st_size > 1000:
        log.info("  using cached eGRID file (%d MB)", RAW.stat().st_size // 1_000_000)
        return RAW.read_bytes()
    log.info("  downloading eGRID 2022 (~15 MB)...")
    session = http_session("AquiferWatch-eGRID/0.1")
    r = session.get(EGRID_URL, timeout=180)
    r.raise_for_status()
    RAW.write_bytes(r.content)
    log.info("  saved to %s (%d MB)", RAW, len(r.content) // 1_000_000)
    return r.content


def parse() -> pd.DataFrame:
    """Parse the ST22 (state summary) sheet.

    eGRID state-level CO2 output emission rate is column `STCO2RTA`
    (lb CO2 / MWh, all fuels). Column positions drift between releases — we
    locate by column name rather than index.
    """
    content = download()
    xlsx = pd.ExcelFile(BytesIO(content))
    # Pick the state sheet — name includes "ST" (e.g. ST22)
    state_sheets = [s for s in xlsx.sheet_names if s.upper().startswith("ST")]
    if not state_sheets:
        raise RuntimeError(f"no state sheet found; sheets: {xlsx.sheet_names}")
    sheet = state_sheets[0]
    # eGRID ST sheet: row 0 = friendly names, row 1 = machine codes (PSTATABB, STCO2RTA, …). Use codes.
    df = xlsx.parse(sheet, header=1)
    df.columns = [str(c).strip() for c in df.columns]

    if "PSTATABB" not in df.columns:
        raise RuntimeError(f"PSTATABB missing; columns: {df.columns.tolist()[:10]}")
    if "STCO2RTA" not in df.columns:
        raise RuntimeError(f"STCO2RTA missing; columns: {df.columns.tolist()[:10]}")

    out = df[["PSTATABB", "STCO2RTA"]].rename(
        columns={"PSTATABB": "state", "STCO2RTA": "co2_lb_per_mwh"}
    )
    out["co2_lb_per_mwh"] = pd.to_numeric(out["co2_lb_per_mwh"], errors="coerce")
    out = out.dropna(subset=["co2_lb_per_mwh"]).drop_duplicates(subset=["state"])
    # 1 lb/MWh = 0.453592 kg / 1000 kWh = 0.000453592 kg/kWh
    out["co2_kg_per_kwh"] = out["co2_lb_per_mwh"] * 0.453592 / 1000.0
    out["year"] = 2022

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT, index=False)
    log.info("Wrote %d states → %s", len(out), OUTPUT)
    return out


if __name__ == "__main__":
    parse()
