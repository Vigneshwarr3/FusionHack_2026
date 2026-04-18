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
