"""Enrich baseline.parquet with model predictions + years-until-uneconomic.

Runs AFTER `compose_baseline.py` (which builds the data-driven baseline) and
AFTER `persist_predictions.py` (which trains NB02 and dumps per-county
forecasts). Merges the predictions onto the baseline and precomputes the
two scalars the map hero visual wants:

    annual_decline_m_pred            CatBoost one-step decline forecast
    decline_lo_m, decline_hi_m       80% conformal bounds on that decline
    years_until_uneconomic           (thickness - 9m) / |decline|, using the
                                     predicted decline where we have one, else
                                     the heuristic `annual_decline_m`.
    years_until_uneconomic_lo/hi     Same, using the conformal bands (lo =
                                     pessimistic decline, hi = optimistic).
    decline_source                   'model' | 'heuristic' — so the UI can
                                     show "modeled" vs. "observed linear-slope".

Writes back over `data/processed/baseline.parquet`. Safe to re-run.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from aquiferwatch.analytics.scenarios import THICKNESS_THRESHOLD_M
from aquiferwatch.config import DATA_DIR

PROCESSED = DATA_DIR / "processed"
BASELINE = PROCESSED / "baseline.parquet"
PREDICTIONS = PROCESSED / "county_predictions.parquet"


def _years_until(thickness_m: pd.Series, decline_m_yr: pd.Series) -> pd.Series:
    """Headroom (thickness - 9m) divided by absolute decline rate.

    Matches analytics/depletion math:
        decline < 0 → depleting, years = headroom / |decline|
        decline ≥ 0 → stable/rising → capped at 1000 (sentinel for "∞")
        thickness already ≤ 9m → 0
    """
    headroom = (thickness_m - THICKNESS_THRESHOLD_M).clip(lower=0)
    depleting = decline_m_yr < 0
    rate = (-decline_m_yr).where(depleting, other=1e-9)
    return (headroom / rate).clip(upper=1000.0)


def enrich() -> pd.DataFrame:
    if not BASELINE.exists():
        raise FileNotFoundError(
            f"{BASELINE} missing — run compose_baseline.py first"
        )
    baseline = pd.read_parquet(BASELINE)
    baseline["fips"] = baseline["fips"].astype(str).str.zfill(5)

    if PREDICTIONS.exists():
        preds = pd.read_parquet(PREDICTIONS)
        preds["fips"] = preds["fips"].astype(str).str.zfill(5)
        keep = [
            "fips",
            "annual_decline_m_pred",
            "decline_lo_m",
            "decline_hi_m",
            "thickness_pred_next_m",
            "model_id",
            "coverage_target",
        ]
        preds = preds[keep]
    else:
        print(f"  (no {PREDICTIONS.name} — decline_source will be all 'heuristic')")
        preds = pd.DataFrame(columns=["fips"])

    merged = baseline.merge(preds, on="fips", how="left")

    # Decline we actually use for years-until:
    #   model pred where available, else the composed heuristic.
    has_pred = merged["annual_decline_m_pred"].notna() if "annual_decline_m_pred" in merged else pd.Series(False, index=merged.index)
    if "annual_decline_m_pred" not in merged:
        merged["annual_decline_m_pred"] = np.nan
        merged["decline_lo_m"] = np.nan
        merged["decline_hi_m"] = np.nan
    decline_effective = merged["annual_decline_m_pred"].where(has_pred, merged["annual_decline_m"])
    decline_lo_eff = merged["decline_lo_m"].where(has_pred, merged["annual_decline_m"])
    decline_hi_eff = merged["decline_hi_m"].where(has_pred, merged["annual_decline_m"])

    merged["decline_source"] = np.where(has_pred, "model", "heuristic")
    merged["years_until_uneconomic"] = _years_until(
        merged["saturated_thickness_m"], decline_effective
    )
    # lo band = more-depleting decline (lower bound of decline is more negative)
    # → fewer years until uneconomic. Swap so lo/hi on YEARS is intuitive (lo = pessimistic).
    merged["years_until_uneconomic_lo"] = _years_until(
        merged["saturated_thickness_m"], decline_lo_eff
    )
    merged["years_until_uneconomic_hi"] = _years_until(
        merged["saturated_thickness_m"], decline_hi_eff
    )

    merged.to_parquet(BASELINE, index=False)
    n_model = int(has_pred.sum())
    print(f"enriched {BASELINE.relative_to(DATA_DIR.parent)}")
    print(f"  {n_model} / {len(merged)} counties have model predictions")
    print(f"  years_until_uneconomic: p10={merged['years_until_uneconomic'].quantile(0.1):.1f}  "
          f"median={merged['years_until_uneconomic'].median():.1f}  "
          f"p90={merged['years_until_uneconomic'].quantile(0.9):.1f}")
    return merged


if __name__ == "__main__":
    enrich()
