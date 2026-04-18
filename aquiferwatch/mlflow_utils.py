"""Shared MLflow wrapper.

Every training / imputation / scenario run in a notebook must go through `start_run`.
This is the contract that makes teammates' experiments visible to each other.

Standard tags logged on every run:
  - team_member:   who ran it  (from AQW_TEAM_MEMBER env var)
  - module:        which analytics module (depletion, extraction_imputation, scenarios, ...)
  - scenario:      scenario id if applicable, else "n/a"
  - git_sha:       best-effort short sha for reproducibility

Usage:
    from aquiferwatch.mlflow_utils import start_run

    with start_run(module="depletion", run_name="kansas_linear_baseline") as run:
        mlflow.log_params({...})
        mlflow.log_metrics({...})
        mlflow.log_artifact("county_forecasts.parquet")
"""

from __future__ import annotations

import subprocess
from contextlib import contextmanager
from typing import Iterator

import mlflow

from aquiferwatch.config import settings


def _git_short_sha() -> str:
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
        )
    except Exception:
        return "unknown"


def configure() -> None:
    """Point MLflow at the configured tracking URI. Idempotent."""
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)


@contextmanager
def start_run(
    *,
    module: str,
    run_name: str,
    experiment: str = "aquiferwatch",
    scenario: str = "n/a",
    extra_tags: dict[str, str] | None = None,
) -> Iterator[mlflow.ActiveRun]:
    """Start an MLflow run with the team's standard tag set.

    Prefer this over `mlflow.start_run()` directly — it enforces the tag contract.
    """
    configure()
    mlflow.set_experiment(experiment)
    tags = {
        "team_member": settings.team_member,
        "module": module,
        "scenario": scenario,
        "git_sha": _git_short_sha(),
    }
    if extra_tags:
        tags.update(extra_tags)
    with mlflow.start_run(run_name=run_name, tags=tags) as run:
        yield run
