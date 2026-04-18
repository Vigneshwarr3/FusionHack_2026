"""Unified wrappers over XGBoost / LightGBM / CatBoost + stacking.

Design goal: keep the notebook thin. Vignesh swaps hyperparameters / adds
frameworks / tweaks stacking meta-learners — all the plumbing (fit, predict,
metrics, MLflow logging) lives here.

Usage
-----
    from aquiferwatch.analytics.models import (
        BoostedRegressor, StackingEnsemble, train_and_log, evaluate,
    )

    xgb = BoostedRegressor("xgboost",  params={"max_depth": 6, "n_estimators": 500})
    lgb = BoostedRegressor("lightgbm", params={"num_leaves": 63, "n_estimators": 500})
    cat = BoostedRegressor("catboost", params={"iterations": 500, "depth": 6, "verbose": 0})

    ensemble = StackingEnsemble(base=[xgb, lgb, cat], meta="ridge")

    for name, model in [("xgb", xgb), ("lgb", lgb), ("cat", cat), ("stack", ensemble)]:
        train_and_log(
            model, X_train, y_train, X_test, y_test,
            module="extraction_imputation", run_name=f"tx_{name}_v1",
        )

Quantile bands (uncertainty)
----------------------------
LightGBM and XGBoost both support `objective="quantile"`. CatBoost uses
`MultiQuantile`. Use `QuantileBoostedRegressor` to fit p10/p50/p90 together.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

import mlflow
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GroupKFold, KFold

from aquiferwatch.mlflow_utils import start_run

Framework = Literal["xgboost", "lightgbm", "catboost"]


# ---------------------------------------------------------------------------
# Base regressor
# ---------------------------------------------------------------------------
@dataclass
class BoostedRegressor:
    """Uniform interface over XGBoost / LightGBM / CatBoost.

    `params` is passed directly to the underlying framework — consult each
    library's docs for the full knob set. The only params we fix ourselves:
    - `random_state=42` unless overridden
    - verbose kept low on CatBoost by default
    """

    framework: Framework
    params: dict[str, Any] = field(default_factory=dict)
    model: Any = field(default=None, init=False, repr=False)

    def _build(self):
        p = {"random_state": 42, **self.params}
        if self.framework == "xgboost":
            from xgboost import XGBRegressor

            return XGBRegressor(**p)
        if self.framework == "lightgbm":
            from lightgbm import LGBMRegressor

            return LGBMRegressor(**p)
        if self.framework == "catboost":
            from catboost import CatBoostRegressor

            p.setdefault("verbose", 0)
            return CatBoostRegressor(**p)
        raise ValueError(f"unknown framework: {self.framework}")

    def fit(self, X: pd.DataFrame, y: pd.Series, **fit_kwargs):
        self.model = self._build()
        self.model.fit(X, y, **fit_kwargs)
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return self.model.predict(X)


# ---------------------------------------------------------------------------
# Quantile regressor (p10 / p50 / p90 for uncertainty bands)
# ---------------------------------------------------------------------------
@dataclass
class QuantileBoostedRegressor:
    """Fits three quantile regressors in one call. Matches the pattern in
    Agricultural_Data_Analysis/backend/models/yield_model.py so the API contract
    (p10/p50/p90) is consistent across the parent project."""

    framework: Framework
    params: dict[str, Any] = field(default_factory=dict)
    quantiles: tuple[float, float, float] = (0.1, 0.5, 0.9)
    models: dict[float, Any] = field(default_factory=dict, init=False, repr=False)

    def fit(self, X: pd.DataFrame, y: pd.Series):
        for q in self.quantiles:
            m = self._build(q)
            m.fit(X, y)
            self.models[q] = m
        return self

    def predict(self, X: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({f"p{int(q*100)}": m.predict(X) for q, m in self.models.items()})

    def _build(self, q: float):
        p = {"random_state": 42, **self.params}
        if self.framework == "lightgbm":
            from lightgbm import LGBMRegressor

            return LGBMRegressor(objective="quantile", alpha=q, **p)
        if self.framework == "xgboost":
            from xgboost import XGBRegressor

            # xgboost >=2.0 supports "reg:quantileerror"
            return XGBRegressor(objective="reg:quantileerror", quantile_alpha=q, **p)
        if self.framework == "catboost":
            from catboost import CatBoostRegressor

            p.setdefault("verbose", 0)
            return CatBoostRegressor(loss_function=f"Quantile:alpha={q}", **p)
        raise ValueError(f"unknown framework: {self.framework}")


# ---------------------------------------------------------------------------
# Stacking ensemble
# ---------------------------------------------------------------------------
@dataclass
class StackingEnsemble:
    """Out-of-fold stacking. Base models predict on held-out folds, the
    meta-learner fits on the stacked OOF predictions.

    Meta-learner options: "ridge" (default, robust), "mean" (unweighted avg,
    no meta-learner at all — useful as a sanity baseline)."""

    base: list[BoostedRegressor]
    meta: Literal["ridge", "mean"] = "ridge"
    n_folds: int = 5
    meta_model: Any = field(default=None, init=False, repr=False)
    fitted_base: list[BoostedRegressor] = field(default_factory=list, init=False, repr=False)

    def fit(self, X: pd.DataFrame, y: pd.Series, groups: pd.Series | None = None):
        """Fit base learners out-of-fold, then the meta-learner on stacked OOF preds.

        Pass `groups` (e.g. county FIPS) to use GroupKFold. This is the correct
        choice for AquiferWatch — same-county rows from different years share
        hidden state (well network, geology), and plain KFold leaks that
        across folds, inflating OOF scores.
        """
        if groups is not None:
            kf = GroupKFold(n_splits=self.n_folds)
            splitter = kf.split(X, y, groups)
        else:
            kf = KFold(n_splits=self.n_folds, shuffle=True, random_state=42)
            splitter = kf.split(X)
        oof = np.zeros((len(X), len(self.base)))
        for tr_idx, va_idx in splitter:
            Xt, yt = X.iloc[tr_idx], y.iloc[tr_idx]
            Xv = X.iloc[va_idx]
            for j, bm in enumerate(self.base):
                m = BoostedRegressor(framework=bm.framework, params=bm.params)
                m.fit(Xt, yt)
                oof[va_idx, j] = m.predict(Xv)

        # Now refit each base on all data
        self.fitted_base = []
        for bm in self.base:
            m = BoostedRegressor(framework=bm.framework, params=bm.params).fit(X, y)
            self.fitted_base.append(m)

        if self.meta == "ridge":
            self.meta_model = Ridge(alpha=1.0).fit(oof, y)
        # mean ensemble has no meta model to fit
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        base_preds = np.column_stack([m.predict(X) for m in self.fitted_base])
        if self.meta == "mean":
            return base_preds.mean(axis=1)
        return self.meta_model.predict(base_preds)


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------
def evaluate(y_true: pd.Series, y_pred: np.ndarray) -> dict[str, float]:
    return {
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
        "mape": float(np.mean(np.abs((y_true - y_pred) / np.where(y_true == 0, 1, y_true)))),
    }


def quantile_coverage(y_true: pd.Series, preds: pd.DataFrame) -> dict[str, float]:
    """Fraction of y_true falling inside [p10, p90]. Target: ~0.80."""
    yt = np.asarray(y_true)
    p10 = preds["p10"].to_numpy()
    p90 = preds["p90"].to_numpy()
    inside = ((yt >= p10) & (yt <= p90)).mean()
    return {"p10_p90_coverage": float(inside)}


# ---------------------------------------------------------------------------
# Train + log (the one-liner for notebooks)
# ---------------------------------------------------------------------------
def train_and_log(
    model,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    *,
    module: str,
    run_name: str,
    groups_train: pd.Series | None = None,
    extra_params: dict[str, Any] | None = None,
) -> dict[str, float]:
    """Fit `model`, evaluate on test, log everything to MLflow. Returns metrics.

    For StackingEnsemble, pass `groups_train` (FIPS) so its OOF folds are
    county-grouped. For single models this argument is ignored.
    """
    with start_run(module=module, run_name=run_name) as run:
        mlflow.log_params(_flatten_model_params(model))
        if extra_params:
            mlflow.log_params(extra_params)
        if isinstance(model, StackingEnsemble) and groups_train is not None:
            model.fit(X_train, y_train, groups=groups_train)
        else:
            model.fit(X_train, y_train)

        if isinstance(model, QuantileBoostedRegressor):
            preds = model.predict(X_test)
            metrics = evaluate(y_test, preds["p50"].values)
            metrics.update(quantile_coverage(y_test, preds))
        else:
            preds = model.predict(X_test)
            metrics = evaluate(y_test, preds)

        mlflow.log_metrics(metrics)
        mlflow.set_tag("feature_count", str(X_train.shape[1]))
        mlflow.set_tag("train_rows", str(len(X_train)))
        print(f"[{run_name}] metrics = {metrics}")
        return metrics


def _flatten_model_params(model) -> dict[str, Any]:
    if isinstance(model, BoostedRegressor):
        return {"framework": model.framework, **_kv(model.params, prefix="p_")}
    if isinstance(model, QuantileBoostedRegressor):
        return {
            "framework": model.framework,
            "quantiles": list(model.quantiles),
            **_kv(model.params, prefix="p_"),
        }
    if isinstance(model, StackingEnsemble):
        return {
            "stack_meta": model.meta,
            "stack_folds": model.n_folds,
            "stack_base": [m.framework for m in model.base],
        }
    return {}


def _kv(d: dict, prefix: str = "") -> dict:
    return {f"{prefix}{k}": v for k, v in d.items()}


# ---------------------------------------------------------------------------
# Comparison helper
# ---------------------------------------------------------------------------
def spatial_cv_scores(
    model_factory,
    X: pd.DataFrame,
    y: pd.Series,
    groups: pd.Series,
    n_folds: int = 5,
) -> pd.DataFrame:
    """Run GroupKFold-CV on `fips` and return per-fold metrics.

    This is the acceptance protocol (per docs/methodology.md §6):
    whole-county holdouts, no spatial leakage. A model that looks good under
    random-row CV but falls apart under spatial CV is overfitting to county
    identity — do not deploy it.

    Parameters
    ----------
    model_factory : callable () -> model
        Returns a fresh unfit model per fold. e.g. `lambda: BoostedRegressor("lightgbm", params=...)`.
    """
    kf = GroupKFold(n_splits=n_folds)
    rows = []
    for fold, (tr_idx, va_idx) in enumerate(kf.split(X, y, groups)):
        Xt, yt = X.iloc[tr_idx], y.iloc[tr_idx]
        Xv, yv = X.iloc[va_idx], y.iloc[va_idx]
        model = model_factory()
        model.fit(Xt, yt)
        if isinstance(model, QuantileBoostedRegressor):
            preds = model.predict(Xv)
            metrics = evaluate(yv, preds["p50"].values)
            metrics.update(quantile_coverage(yv, preds))
        else:
            metrics = evaluate(yv, model.predict(Xv))
        metrics["fold"] = fold
        rows.append(metrics)
    return pd.DataFrame(rows)


def summarize_cv(cv_df: pd.DataFrame) -> pd.Series:
    """Collapse fold-level metrics into mean ± std. Low std is a signal of
    robustness; high std means the model's quality depends heavily on which
    counties were held out."""
    out = {}
    for col in cv_df.columns:
        if col == "fold":
            continue
        out[f"{col}_mean"] = cv_df[col].mean()
        out[f"{col}_std"] = cv_df[col].std()
    return pd.Series(out)
