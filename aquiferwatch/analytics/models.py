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
# Conformal quantile regressor (calibrated uncertainty bands)
# ---------------------------------------------------------------------------
@dataclass
class ConformalQuantileRegressor:
    """Split-conformal calibration on top of ``QuantileBoostedRegressor``.

    Raw LightGBM / XGBoost / CatBoost quantile regressors routinely produce
    bands narrower than their nominal alpha \u2014 we saw p10/p90 coverage of
    ~0.35 when 0.80 was requested. Split-conformal wraps the underlying
    quantile model with a post-hoc calibration step whose marginal coverage
    is *guaranteed* to be :math:`\\ge 1 - \\alpha` in finite samples
    (Vovk et al. 2005; Romano et al. 2019 "Conformalized Quantile Regression").

    Algorithm
    ---------
    1. Split training data into proper-train (``1 - cal_frac``) and
       calibration (``cal_frac``) halves. When ``groups`` is passed the split
       is by whole county, never by row.
    2. Fit p_lo (alpha=``alpha_lo``) and p_hi (alpha=``alpha_hi``) on
       proper-train only.
    3. On the calibration set, compute the one-sided non-conformity score
       :math:`s_i = \\max(p_{lo}(x_i) - y_i,\\; y_i - p_{hi}(x_i))`.
    4. Let :math:`\\hat q = \\mathrm{quantile}(s, 1 - \\alpha_{\\text{target}})`
       using the standard finite-sample correction.
    5. At inference return bands :math:`[p_{lo}(x) - \\hat q,\\; p_{hi}(x) + \\hat q]`.

    Choose ``alpha_lo / alpha_hi`` slightly inside the target band
    (e.g. 0.1/0.9 for an 80% target) \u2014 the conformal shift widens them
    exactly enough to hit target coverage. Too-wide nominal alphas cause
    over-coverage and wider-than-necessary bands.
    """

    framework: Framework
    params: dict[str, Any] = field(default_factory=dict)
    alpha_lo: float = 0.1
    alpha_hi: float = 0.9
    coverage_target: float = 0.80
    cal_frac: float = 0.3
    seed: int = 42
    quantile: QuantileBoostedRegressor | None = field(default=None, init=False, repr=False)
    q_hat: float = field(default=0.0, init=False)

    def fit(self, X: pd.DataFrame, y: pd.Series, groups: pd.Series | None = None):
        rng = np.random.default_rng(self.seed)
        if groups is not None:
            g = pd.Series(np.asarray(groups), index=X.index)
            uniq = np.array(sorted(g.unique()))
            rng.shuffle(uniq)
            n_cal = max(1, int(round(self.cal_frac * len(uniq))))
            cal_groups = set(uniq[:n_cal])
            cal_mask = g.isin(cal_groups).to_numpy()
        else:
            n = len(X)
            n_cal = max(1, int(round(self.cal_frac * n)))
            perm = rng.permutation(n)
            cal_mask = np.zeros(n, dtype=bool)
            cal_mask[perm[:n_cal]] = True

        X_cal, y_cal = X.iloc[cal_mask], y.iloc[cal_mask]
        X_fit, y_fit = X.iloc[~cal_mask], y.iloc[~cal_mask]

        self.quantile = QuantileBoostedRegressor(
            framework=self.framework,
            params=self.params,
            quantiles=(self.alpha_lo, 0.5, self.alpha_hi),
        )
        self.quantile.fit(X_fit, y_fit)

        cal_preds = self.quantile.predict(X_cal)
        lo_key = f"p{int(self.alpha_lo * 100)}"
        hi_key = f"p{int(self.alpha_hi * 100)}"
        lo = cal_preds[lo_key].to_numpy()
        hi = cal_preds[hi_key].to_numpy()
        yc = np.asarray(y_cal)
        scores = np.maximum(lo - yc, yc - hi)

        # Finite-sample conformal level: ceil((n_cal + 1) * (1 - alpha)) / n_cal
        n_cal_actual = len(scores)
        alpha = 1.0 - self.coverage_target
        k = int(np.ceil((n_cal_actual + 1) * (1.0 - alpha)))
        k = min(max(k, 1), n_cal_actual)
        self.q_hat = float(np.sort(scores)[k - 1])
        return self

    def predict(self, X: pd.DataFrame) -> pd.DataFrame:
        assert self.quantile is not None, "fit first"
        raw = self.quantile.predict(X)
        lo_key = f"p{int(self.alpha_lo * 100)}"
        hi_key = f"p{int(self.alpha_hi * 100)}"
        lo_name = f"p{int(round((1 - self.coverage_target) / 2 * 100))}"
        hi_name = f"p{int(round((1 - (1 - self.coverage_target) / 2) * 100))}"
        out = pd.DataFrame(
            {
                lo_name: raw[lo_key].to_numpy() - self.q_hat,
                "p50": raw["p50"].to_numpy(),
                hi_name: raw[hi_key].to_numpy() + self.q_hat,
            }
        )
        # Also expose canonical p10 / p90 names so `quantile_coverage` works unchanged.
        out["p10"] = out[lo_name]
        out["p90"] = out[hi_name]
        return out


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

        if isinstance(model, (QuantileBoostedRegressor, ConformalQuantileRegressor)):
            preds = model.predict(X_test)
            metrics = evaluate(y_test, preds["p50"].values)
            metrics.update(quantile_coverage(y_test, preds))
            if isinstance(model, ConformalQuantileRegressor):
                metrics["conformal_q_hat"] = model.q_hat
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
    if isinstance(model, ConformalQuantileRegressor):
        return {
            "framework": model.framework,
            "alpha_lo": model.alpha_lo,
            "alpha_hi": model.alpha_hi,
            "coverage_target": model.coverage_target,
            "cal_frac": model.cal_frac,
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
