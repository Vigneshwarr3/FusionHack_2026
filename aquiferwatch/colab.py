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
    """Wire up env + MLflow. Safe to call multiple times.

    Resolution order for each secret key:
      1. `os.environ` (already exported by the shell)
      2. Google Colab `userdata` (when running in Colab)
      3. A repo-root `.env` file (when running locally in VSCode / Jupyter)
    First one found wins; later sources don't overwrite earlier ones.
    """
    _load_local_dotenv((*SECRET_KEYS, *extra_secrets))
    _load_colab_secrets((*SECRET_KEYS, *extra_secrets))
    os.environ["AQW_TEAM_MEMBER"] = team_member

    ephemeral = False
    if not os.environ.get("MLFLOW_TRACKING_URI"):
        os.environ["MLFLOW_TRACKING_URI"] = "sqlite:////content/mlflow.db"
        ephemeral = True

    import mlflow

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    print(f"team_member       = {team_member}")
    print(f"tracking URI      = {os.environ['MLFLOW_TRACKING_URI']}")

    if ephemeral:
        print()
        print("WARNING: no MLFLOW_TRACKING_URI in Colab Secrets.")
        print("  Runs will be logged to a SQLite file under /content and LOST when this")
        print("  Colab session ends. To persist runs across sessions (and share them")
        print("  with your teammate) see mlflow/setup_dagshub.md - 2-minute setup on")
        print("  DagsHub's free MLflow tier.")


def _load_colab_secrets(keys: Iterable[str]) -> None:
    """Read from google.colab.userdata when available; silently skip otherwise."""
    try:
        from google.colab import userdata  # type: ignore
    except ImportError:
        return  # not in Colab; .env path handles this case

    for k in keys:
        if os.environ.get(k):
            continue
        try:
            v = userdata.get(k)
        except Exception:
            v = None
        if v:
            os.environ[k] = v


def _load_local_dotenv(keys: Iterable[str]) -> None:
    """Read missing keys from a repo-root `.env` file into `os.environ`.

    Pydantic reads `.env` into its own settings object but does *not* export to
    `os.environ`, which means MLflow (which reads env vars directly) can't see
    credentials that only live in `.env`. This function bridges that gap for
    the specific keys `bootstrap()` cares about.

    Skipped when running in Colab (userdata is the canonical source there).
    """
    try:
        import google.colab  # type: ignore # noqa: F401
        return
    except ImportError:
        pass

    # Walk up from this file to find a .env (repo root).
    from pathlib import Path

    here = Path(__file__).resolve()
    for parent in (here.parent, *here.parents):
        candidate = parent / ".env"
        if candidate.is_file():
            dotenv_path = candidate
            break
    else:
        return  # no .env anywhere; leave os.environ alone

    try:
        from dotenv import dotenv_values  # python-dotenv is already a dep
    except ImportError:
        return

    values = dotenv_values(dotenv_path)
    for k in keys:
        if os.environ.get(k):
            continue
        v = values.get(k)
        if v:
            os.environ[k] = v
