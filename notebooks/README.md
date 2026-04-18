# Notebooks — collaboration rules

These rules exist because we're two people running experiments on the same data and we need each other's runs to be visible, reproducible, and comparable.

## Rule 1 — notebooks are thin drivers, logic lives in the package

Do not put substantive logic inside a notebook. A notebook should look roughly like:

```python
from aquiferwatch.mlflow_utils import start_run
from aquiferwatch.analytics.depletion import project_county

# load data
# call package functions
# plot results
```

If you find yourself writing a long function in a cell, promote it to `aquiferwatch/analytics/<module>.py` and import it. This is how the other teammate gets to reuse your work without copy-pasting cells.

## Rule 2 — every run goes through `start_run`

```python
from aquiferwatch.mlflow_utils import start_run
import mlflow

with start_run(module="depletion", run_name="linear_baseline_kansas") as run:
    mlflow.log_params({"min_history_years": 10, "smoothing": "none"})
    # ... training ...
    mlflow.log_metrics({"rmse_test": 0.83, "mae_test": 0.61})
    mlflow.log_artifact("kansas_forecasts.parquet")
```

This tags every run with `team_member`, `module`, `scenario`, and `git_sha` so the MLflow UI shows a clean split by owner and module.

## Rule 3 — naming

- Notebook files: `NN_short_topic.ipynb` — `01_saturated_thickness_kriging.ipynb`, `02_tx_imputation_v1.ipynb`
- MLflow run names: `<module>_<variant>` — `depletion_linear_baseline`, `imputation_lgbm_v2`
- Experiment: always `aquiferwatch` (the wrapper sets this automatically)

## Rule 4 — strip outputs before committing

```bash
poetry run nbstripout --install
```

This installs a git filter so outputs don't pollute diffs. Run once per clone.

## Rule 5 — if you need someone else's notebook, you should not need to read their cells to understand it

Pair a notebook with a one-paragraph header cell: *what question, what data, what method, what conclusion*. That's the contract.

---

## Research traps documented up-front

Things that feel like progress but aren't. Listed here so we don't each discover them separately.

### Modeling

- **XGBoost vs. LightGBM vs. CatBoost is usually a <5% fight on tabular.** They share the GBDT core. Don't spend Day-2 hunting for the "best" one — benchmark once, pick one for iteration, re-benchmark at the end.
- **Stacking three near-identical GBDTs rarely helps much.** If you want ensemble diversity, mix a GBDT with a linear model / KNN / per-group baseline.
- **Per-county GBDT is an overfitting machine** at ~20–30 points per county. Use pooled models with county as a feature or group-level mean encoding.
- **Random-row CV on county-year data is cheating.** Same-county years share geology/infra; splits must be by FIPS (GroupKFold). A model that looks great under random CV but flat under spatial CV is overfitting to county identity.
- **Recursive multi-step forecasting for 25-year projections is a known trap** (M6 result). For long horizons, use direct-multi-output or physics-informed extrapolation, not iterated one-step.
- **Synthetic-data leaderboards don't transfer.** The generators in `aquiferwatch/data/synthetic*.py` have known structure. Treat synthetic scores as pipeline smoke tests, not model-selection signal.

### Foundation models

- **Agricultural foundation models (Presto, Prithvi, SatMAE, CropFM)** operate on satellite imagery, not county tabular data. Integrating them = multi-day pipeline to extract per-county pixel embeddings. Skip unless we've already cleared the accuracy gate and have 2+ days of buffer.
- **TabPFN v2 is worth 30 minutes.** It's the only foundation model that competes with GBDTs on <10k-row tabular. Drop-in API, CPU/GPU. Include it as a fourth model in notebook 01 once GBDT baselines are stable.
- **LLMs / embeddings of county text descriptions** — not applicable here. Don't go looking.

### Evaluation

- **Point accuracy alone is insufficient.** p10/p90 coverage is the acceptance metric for the map's uncertainty rings. Target 0.78–0.82.
- **Don't tune hyperparams on the test set.** Hold out a validation split or use nested CV. `optuna` + GroupKFold is the right pattern when we're ready to sweep.
- **Feature importance plots lie about causal structure.** They report what the model uses, not what actually drives the outcome. Don't use them for policy claims.

