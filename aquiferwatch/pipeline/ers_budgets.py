"""USDA ERS — commodity costs and returns (gross revenue per acre).

Downloads per-commodity XLSX files published by ERS and extracts the
"Gross value of production" row for the most recent year.

Output: data/processed/ers_revenue_per_acre.parquet
Columns: crop, year, gross_value_usd_per_acre, source_file
"""

from __future__ import annotations

from io import BytesIO

import pandas as pd

from aquiferwatch.config import DATA_DIR
from aquiferwatch.pipeline.common import get_logger, http_session

log = get_logger(__name__)

# Discovered by scraping https://www.ers.usda.gov/data-products/commodity-costs-and-returns
BASE = "https://www.ers.usda.gov/media"
FILES: dict[str, str] = {
    "corn":     f"{BASE}/4961/corn.xlsx",
    "cotton":   f"{BASE}/4963/cotton.xlsx",
    "sorghum":  f"{BASE}/4971/sorghum.xlsx",
    "soybeans": f"{BASE}/4975/soybeans.xlsx",
    "wheat":    f"{BASE}/4977/wheat.xlsx",
    # Alfalfa/hay isn't broken out in ERS costs-and-returns — handled as a
    # constant in scenarios.py, documented in docs/limitations.md.
}

RAW_DIR = DATA_DIR / "raw" / "ers"
OUTPUT = DATA_DIR / "processed" / "ers_revenue_per_acre.parquet"


def _download(crop: str, url: str) -> bytes:
    cache = RAW_DIR / f"{crop}.xlsx"
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if cache.exists() and cache.stat().st_size > 1000:
        return cache.read_bytes()
    session = http_session("AquiferWatch-ERS/0.1")
    r = session.get(url, timeout=60)
    r.raise_for_status()
    cache.write_bytes(r.content)
    return r.content


def _extract_gross_value(xlsx_bytes: bytes, crop: str) -> tuple[int | None, float | None]:
    """Extract latest US-total gross value of production from ERS machine-readable sheet.

    The 'Data sheet (machine readable)' has a clean columnar layout with
    Commodity / Category / Item / Region / Year / Value columns. Filter to
    `Category == 'Gross value of production'` + `Region == 'U.S. total'` and
    take the most recent Year.
    """
    df = pd.read_excel(BytesIO(xlsx_bytes), sheet_name="Data sheet (machine readable)")
    # Some ERS files use slightly different Item labels (e.g. "Primary product, grain"
    # for corn, "Primary product" generally). Filter only by Category + Region.
    sub = df[
        (df["Category"].str.strip() == "Gross value of production")
        & (df["Region"].str.strip() == "U.S. total")
    ]
    # Take the row with the largest "Total, gross value of production" style row if present,
    # else just take max year.
    totals = sub[sub["Item"].astype(str).str.contains("Total", case=False, na=False)]
    target = totals if not totals.empty else sub

    target = target.dropna(subset=["Year", "Value"])
    target["Year"] = pd.to_numeric(target["Year"], errors="coerce")
    target["Value"] = pd.to_numeric(target["Value"], errors="coerce")
    target = target.dropna(subset=["Year", "Value"])
    if target.empty:
        log.warning("  [%s] no Gross value rows after filtering", crop)
        return None, None
    latest = target.sort_values("Year").iloc[-1]
    return int(latest["Year"]), float(latest["Value"])


def fetch_all() -> pd.DataFrame:
    rows = []
    for crop, url in FILES.items():
        try:
            content = _download(crop, url)
            year, value = _extract_gross_value(content, crop)
        except Exception as e:
            log.warning("  [%s] failed: %s", crop, e)
            continue
        if year is None or value is None:
            continue
        log.info("  [%s] %d: $%.0f/acre gross", crop, year, value)
        rows.append({
            "crop": crop,
            "year": year,
            "gross_value_usd_per_acre": value,
            "source_file": url.rsplit("/", 1)[1],
        })
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT, index=False)
    log.info("Wrote %d ERS revenue rows → %s", len(out), OUTPUT)
    return out


if __name__ == "__main__":
    fetch_all()
