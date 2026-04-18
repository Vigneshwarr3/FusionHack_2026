"""Smoke tests for the synthetic datasets.

These cover the shape + no-leakage invariants the notebooks depend on.
Model-training smoke is deferred to tests/test_models.py (installs heavier deps).
"""

from __future__ import annotations

from aquiferwatch.data.synthetic import (
    FEATURE_COLS,
    TARGET_COL,
    generate_tx_extraction_dataset,
    spatial_train_test_split,
)
from aquiferwatch.data.synthetic_depletion import (
    FEATURE_COLS as DEP_FEATURE_COLS,
    TARGET_COL as DEP_TARGET_COL,
    build_lagged_features,
    generate_thickness_history,
)


def test_tx_dataset_shape():
    df = generate_tx_extraction_dataset(n_counties=10, n_years=5, seed=0)
    assert len(df) == 50
    for col in FEATURE_COLS + [TARGET_COL]:
        assert col in df.columns
    # No NaNs and pumpage strictly positive
    assert not df[FEATURE_COLS].isna().any().any()
    assert (df[TARGET_COL] > 0).all()


def test_tx_spatial_split_no_leakage():
    df = generate_tx_extraction_dataset(n_counties=20, n_years=5, seed=1)
    train, test = spatial_train_test_split(df, test_frac=0.25)
    assert set(train["fips"]).isdisjoint(set(test["fips"]))


def test_depletion_lag_features_no_nan():
    history = generate_thickness_history(n_counties=10, n_years=15, seed=2)
    feat = build_lagged_features(history)
    for col in DEP_FEATURE_COLS + [DEP_TARGET_COL]:
        assert col in feat.columns
    assert not feat[DEP_FEATURE_COLS + [DEP_TARGET_COL]].isna().any().any()
