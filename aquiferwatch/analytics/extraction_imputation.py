"""LightGBM / XGBoost / CatBoost imputation for counties with unreported extractions.

Training signal: Kansas + Nebraska metered pumpage (from kgs_wimas + ne_dnr).
Features: irrigated acreage by crop, crop mix, saturated thickness, well density,
PRISM precipitation, ERS crop price signals.

The model experimentation happens in `notebooks/01_tx_extraction_imputation.ipynb`
using the shared wrappers in `aquiferwatch.analytics.models`. This module owns
the *production* training + inference entrypoints once Vignesh picks a winner.

Every training run goes through `aquiferwatch.mlflow_utils.start_run(module="extraction_imputation")`.
"""

from __future__ import annotations

import pandas as pd


def train_imputation_model(train: pd.DataFrame, val: pd.DataFrame) -> dict:
    """Train LightGBM regressor; return metrics + model artifact path."""
    # TODO(day-3): LightGBM w/ quantile regression for uncertainty bands.
    # Follow the pattern in Agricultural_Data_Analysis/backend/models/yield_model.py
    # (3 quantile models at p10/p50/p90). Log to MLflow under experiment "aquiferwatch".
    raise NotImplementedError("Day 3 — teammate owns")


def predict_extraction(model_uri: str, features: pd.DataFrame) -> pd.DataFrame:
    """Apply the trained model to counties missing metered data."""
    raise NotImplementedError("Day 3 — teammate owns")
