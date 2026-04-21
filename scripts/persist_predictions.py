"""Train NB02's winning depletion model on the full panel and persist
per-county predictions + conformal uncertainty bands to parquet.

Why this script exists
----------------------
NB02 trains CatBoost + a conformal quantile wrapper and reports R² in the
notebook, but does not write predictions back to disk. The web/API layer
needs those predictions joined onto the baseline so the frontend can show
"our R²=0.93 model forecasts X m/yr decline in county Y with [lo, hi] 80%
bands". This script reproduces the notebook's modeling recipe and dumps:

    data/processed/county_predictions.parquet
        fips                      5-digit county FIPS
        thickness_current_m       most recent observed thickness
        thickness_pred_next_m     CatBoost one-step-ahead forecast
        annual_decline_m_pred     thickness_pred_next_m - thickness_current_m
        decline_lo_m, decline_hi_m   80% conformal bounds on that decline
        model_id                  e.g. 'catboost_v1'
        coverage_target           conformal target (0.80)

Only counties with ≥ min_points_per_county in the TWDB+WIZARD panel get
predictions (roughly the 'modeled_high' set). The rest keep heuristic
decline from compose_baseline.

Usage
-----
    poetry run python scripts/persist_predictions.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from aquiferwatch.analytics.models import (
    BoostedRegressor,
    ConformalQuantileRegressor,
    evaluate,
)
from aquiferwatch.config import DATA_DIR
from aquiferwatch.data.real import (
    REAL_DEPLETION_FEATURE_COLS as FEATURE_COLS,
    REAL_DEPLETION_TARGET_COL as TARGET_COL,
    build_depletion_history,
    build_lagged_features,
)

OUTPUT = DATA_DIR / "processed" / "county_predictions.parquet"

CAT_PARAMS = {
    "iterations": 700,
    "depth": 5,
    "learning_rate": 0.05,
    "l2_leaf_reg": 3.0,
    "verbose": 0,
}
LGB_PARAMS = {
    "n_estimators": 400,
    "num_leaves": 31,
    "learning_rate": 0.05,
    "min_child_samples": 20,
    "subsample": 0.9,
    "colsample_bytree": 0.9,
    "verbose": -1,
}


def _latest_features_per_county(feat: pd.DataFrame) -> pd.DataFrame:
    """Pick the most-recent feature row per county.

    `build_lagged_features` already drops rows where any lag or `y_next_thickness`
    is NaN, which means the `year` of each row is the year whose *next* year we
    have observed. For the forward prediction we want the most recent row per
    county (any row is a valid feature snapshot — last one gives us the latest
    information).
    """
    return feat.sort_values(["fips", "year"]).groupby("fips").tail(1).reset_index(drop=True)


def _latest_observed_thickness(history: pd.DataFrame) -> pd.DataFrame:
    """Most recent observed thickness per county (not necessarily the same year
    as the latest feature row — thickness series extend one step past features).
    """
    latest = history.sort_values(["fips", "year"]).groupby("fips").tail(1)
    return latest[["fips", "year", "saturated_thickness_m"]].rename(
        columns={"year": "latest_year", "saturated_thickness_m": "thickness_current_m"}
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-points", type=int, default=6,
                        help="Minimum observations per county to include (default 6)")
    parser.add_argument("--coverage", type=float, default=0.80,
                        help="Conformal coverage target (default 0.80)")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    print(f"building depletion panel (min_points={args.min_points})…")
    history = build_depletion_history(min_year=1990, min_points_per_county=args.min_points)
    n_counties = history["fips"].nunique()
    n_years = history["year"].nunique()
    print(f"  panel: {n_counties} counties × {n_years} distinct years  ({len(history)} rows)")

    feat = build_lagged_features(history)
    print(f"  modeling rows after lag + dropna: {len(feat)}")

    # --- Split: train on everything except held-out counties, for diagnostics only.
    rng = np.random.default_rng(args.seed)
    counties = feat["fips"].unique()
    rng.shuffle(counties)
    n_test = max(1, int(0.2 * len(counties)))
    test_counties = set(counties[:n_test])
    train_mask = ~feat["fips"].isin(test_counties)
    X_train, y_train = feat.loc[train_mask, FEATURE_COLS], feat.loc[train_mask, TARGET_COL]
    X_test, y_test = feat.loc[~train_mask, FEATURE_COLS], feat.loc[~train_mask, TARGET_COL]
    groups_train = feat.loc[train_mask, "fips"]
    print(f"  spatial split: {len(X_train)} train / {len(X_test)} test rows")

    # --- Fit CatBoost for point predictions (NB02's winner).
    print("training CatBoost point regressor…")
    cat = BoostedRegressor("catboost", params=CAT_PARAMS)
    cat.fit(X_train, y_train)
    diag = evaluate(y_test.to_numpy(), cat.predict(X_test))
    print(f"  held-out (spatial) — MAE={diag['mae']:.3f}  RMSE={diag['rmse']:.3f}  R²={diag['r2']:.3f}")

    # Refit on full panel for the final production predictions.
    print("refitting CatBoost on full panel for production predictions…")
    cat_full = BoostedRegressor("catboost", params=CAT_PARAMS)
    cat_full.fit(feat[FEATURE_COLS], feat[TARGET_COL])

    # --- Conformal bands via LightGBM quantile + split-conformal calibration.
    print(f"fitting conformal quantile regressor (coverage={args.coverage})…")
    conf = ConformalQuantileRegressor(
        framework="lightgbm",
        params=LGB_PARAMS,
        alpha_lo=0.1,
        alpha_hi=0.9,
        coverage_target=args.coverage,
        cal_frac=0.3,
        seed=args.seed,
    )
    conf.fit(X_train, y_train, groups=groups_train)
    print(f"  q_hat = {conf.q_hat:.3f}")

    # --- Predict next-year thickness for each county using its most recent features.
    latest_feat = _latest_features_per_county(feat)
    point_preds = cat_full.predict(latest_feat[FEATURE_COLS])
    band_preds = conf.predict(latest_feat[FEATURE_COLS])

    # thickness_current_m = current-year thickness in the feature row
    # (thickness_lag0 isn't a feature but "saturated_thickness_m" is the current val).
    thickness_current = latest_feat["saturated_thickness_m"].to_numpy()
    pred_next = np.asarray(point_preds)
    lo_next = band_preds["p10"].to_numpy()
    hi_next = band_preds["p90"].to_numpy()

    out = pd.DataFrame({
        "fips": latest_feat["fips"].astype(str).str.zfill(5),
        "feature_year": latest_feat["year"].astype(int),
        "thickness_current_m": thickness_current,
        "thickness_pred_next_m": pred_next,
        "thickness_lo_m": lo_next,
        "thickness_hi_m": hi_next,
        "annual_decline_m_pred": pred_next - thickness_current,
        "decline_lo_m": lo_next - thickness_current,
        "decline_hi_m": hi_next - thickness_current,
        "model_id": "catboost_v1",
        "coverage_target": args.coverage,
        "r2_spatial_cv": diag["r2"],
    })

    # If any county has a still-more-recent thickness observation than the
    # feature row (one year ahead), use it as the "current" value for display
    # but keep predictions relative to the modeled pair.
    obs_latest = _latest_observed_thickness(history)
    obs_latest["fips"] = obs_latest["fips"].astype(str).str.zfill(5)
    out = out.merge(obs_latest, on="fips", how="left")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT, index=False)
    print(f"wrote {OUTPUT.relative_to(DATA_DIR.parent)}  ({len(out)} counties)")
    print(f"  decline pred range: [{out['annual_decline_m_pred'].min():.3f}, {out['annual_decline_m_pred'].max():.3f}] m/yr")
    print(f"  mean |decline_hi - decline_lo|: {(out['decline_hi_m'] - out['decline_lo_m']).mean():.3f} m")


if __name__ == "__main__":
    main()
