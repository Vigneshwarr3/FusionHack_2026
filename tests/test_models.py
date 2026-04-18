"""End-to-end smoke for the model wrappers — tiny data, all three frameworks.

Skips gracefully if any boosting library isn't installed (Colab-first env).
"""

from __future__ import annotations

import pytest

from aquiferwatch.data.synthetic import (
    FEATURE_COLS,
    TARGET_COL,
    generate_tx_extraction_dataset,
    spatial_train_test_split,
)


def _split():
    df = generate_tx_extraction_dataset(n_counties=12, n_years=6, seed=3)
    tr, te = spatial_train_test_split(df, test_frac=0.25)
    X_tr, y_tr = tr[FEATURE_COLS], tr[TARGET_COL]
    X_te, y_te = te[FEATURE_COLS], te[TARGET_COL]
    return X_tr, y_tr, X_te, y_te, tr["fips"]


@pytest.mark.parametrize("framework", ["lightgbm", "xgboost", "catboost"])
def test_boosted_regressor_fits(framework):
    pytest.importorskip(framework)
    from aquiferwatch.analytics.models import BoostedRegressor, evaluate

    X_tr, y_tr, X_te, y_te, _ = _split()
    params = {"n_estimators": 30} if framework != "catboost" else {"iterations": 30, "verbose": 0}
    m = BoostedRegressor(framework, params=params).fit(X_tr, y_tr)
    metrics = evaluate(y_te, m.predict(X_te))
    assert "rmse" in metrics and metrics["rmse"] > 0


def test_stacking_ensemble_runs_with_group_cv():
    pytest.importorskip("lightgbm")
    pytest.importorskip("xgboost")
    pytest.importorskip("catboost")
    from aquiferwatch.analytics.models import BoostedRegressor, StackingEnsemble, evaluate

    X_tr, y_tr, X_te, y_te, groups = _split()
    stack = StackingEnsemble(
        base=[
            BoostedRegressor("lightgbm", params={"n_estimators": 30, "verbose": -1}),
            BoostedRegressor("xgboost",  params={"n_estimators": 30}),
            BoostedRegressor("catboost", params={"iterations": 30, "verbose": 0}),
        ],
        meta="ridge",
        n_folds=3,
    ).fit(X_tr, y_tr, groups=groups)
    metrics = evaluate(y_te, stack.predict(X_te))
    assert metrics["rmse"] > 0


def test_quantile_regressor_coverage():
    pytest.importorskip("lightgbm")
    from aquiferwatch.analytics.models import QuantileBoostedRegressor, quantile_coverage

    X_tr, y_tr, X_te, y_te, _ = _split()
    q = QuantileBoostedRegressor("lightgbm", params={"n_estimators": 50, "verbose": -1}).fit(X_tr, y_tr)
    preds = q.predict(X_te)
    assert {"p10", "p50", "p90"}.issubset(preds.columns)
    cov = quantile_coverage(y_te, preds)
    assert 0.0 <= cov["p10_p90_coverage"] <= 1.0
