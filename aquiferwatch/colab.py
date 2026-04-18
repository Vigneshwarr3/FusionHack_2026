"""Colab bootstrap — one cell at the top of every team notebook.

Usage (paste into the first Colab cell):

    !pip -q install git+https://github.com/Vigneshwarr3/FusionHack_2026.git
    from aquiferwatch.colab import bootstrap
    bootstrap(team_member="vignesh")

What it does:
    1. Imports packages (no-op if already installed)
    2. Pulls secrets from Colab userdata (MLFLOW_TRACKING_URI, DAGSHUB_USER_TOKEN,
       AWS_*, DATABASE_URL) into the process env
    3. Sets the AQW_TEAM_MEMBER tag so MLflow runs are attributable
    4. Configures MLflow to point at the shared tracking server

Colab secrets to set (gear icon → Secrets):
    MLFLOW_TRACKING_URI    e.g. https://dagshub.com/vignesh/aquiferwatch.mlflow
    MLFLOW_TRACKING_USERNAME      your username on that server
    MLFLOW_TRACKING_PASSWORD      token / password
    AWS_ACCESS_KEY_ID             (optional — only if reading S3)
    AWS_SECRET_ACCESS_KEY         (optional)

If no MLFLOW_TRACKING_URI is set, falls back to a local sqlite file that
lives in /content so it survives cell re-runs within one session.
"""

from __future__ import annotations

import os
from typing import Iterable

SECRET_KEYS: tuple[str, ...] = (
    "MLFLOW_TRACKING_URI",
    "MLFLOW_TRACKING_USERNAME",
    "MLFLOW_TRACKING_PASSWORD",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "DATABASE_URL",
)


def bootstrap(team_member: str, extra_secrets: Iterable[str] = ()) -> None:
    """Wire up env + MLflow. Safe to call multiple times."""
    _load_colab_secrets((*SECRET_KEYS, *extra_secrets))
    os.environ["AQW_TEAM_MEMBER"] = team_member

    if not os.environ.get("MLFLOW_TRACKING_URI"):
        # Colab-friendly local fallback — keeps runs across cell re-runs
        os.environ["MLFLOW_TRACKING_URI"] = "sqlite:////content/mlflow.db"

    import mlflow

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    print(f"team_member       = {team_member}")
    print(f"tracking URI      = {os.environ['MLFLOW_TRACKING_URI']}")


def _load_colab_secrets(keys: Iterable[str]) -> None:
    """Read from google.colab.userdata when available; silently skip otherwise."""
    try:
        from google.colab import userdata  # type: ignore
    except ImportError:
        return  # not in Colab; caller already has a .env

    for k in keys:
        if os.environ.get(k):
            continue
        try:
            v = userdata.get(k)
        except Exception:
            v = None
        if v:
            os.environ[k] = v
