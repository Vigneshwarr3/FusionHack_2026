# MLflow persistence via DagsHub (2-minute setup)

Problem: the default MLflow tracking URI in Colab is `sqlite:////content/mlflow.db`, which lives inside the Colab VM and **dies when the session ends**. All runs, metrics, artifacts — gone.

Fix: point MLflow at DagsHub's free hosted tracker. Both teammates see the same runs, UI is identical to self-hosted MLflow, zero infra to maintain.

## One-time setup (Raj)

1. Go to **https://dagshub.com**, sign up with GitHub (free).
2. Create a new repo — name it `aquiferwatch` (or anything). Doesn't need to mirror the code; it's just a home for the MLflow server.
3. Open the repo → click **Remote** → **Experiments** → copy the three values shown:
   - `MLFLOW_TRACKING_URI` — looks like `https://dagshub.com/<your-username>/aquiferwatch.mlflow`
   - `MLFLOW_TRACKING_USERNAME` — your DagsHub username
   - `MLFLOW_TRACKING_PASSWORD` — click "Get token" to generate
4. Share the username + token with Vignesh (or have him generate his own by accepting a repo invite).

## Adding the secrets

### Colab (each teammate, one-time per notebook)

Open any notebook → gear icon (top right) → **Secrets** → add the three keys:

| Name | Value |
|---|---|
| `MLFLOW_TRACKING_URI` | `https://dagshub.com/<raj>/aquiferwatch.mlflow` |
| `MLFLOW_TRACKING_USERNAME` | your DagsHub username |
| `MLFLOW_TRACKING_PASSWORD` | your DagsHub access token |

Toggle **Notebook access** on each secret so `bootstrap()` can read them.

Re-run the bootstrap cell — you should see the tracking URI printed and no more "WARNING: runs will be lost" message. Subsequent `train_and_log` calls log to DagsHub.

### Local `.env`

Add to your `.env`:

```
MLFLOW_TRACKING_URI=https://dagshub.com/<raj>/aquiferwatch.mlflow
MLFLOW_TRACKING_USERNAME=<your-dagshub-username>
MLFLOW_TRACKING_PASSWORD=<your-dagshub-token>
```

Restart your Jupyter kernel. The `settings` object in `aquiferwatch.config` picks it up automatically.

## Viewing runs

- **DagsHub web UI**: `https://dagshub.com/<raj>/aquiferwatch/experiments`. Filter by the `team_member`, `module`, or `git_sha` tags that `start_run` applies automatically.
- **MLflow CLI / Python**: works identically to self-hosted MLflow because DagsHub exposes the standard MLflow API. `mlflow.search_runs(...)`, `mlflow.register_model(...)`, etc. all work.

## Limits on the free tier

- 500 MB artifact storage per repo (plenty for parameter dumps + saved models — we don't log big datasets).
- Unlimited runs and metrics.
- Model registry works. You can promote runs to `Staging` / `Production` stages the same way you would on a self-hosted server.

## If you outgrow DagsHub

Option D in [README.md](README.md) — self-host MLflow on the parent project's EC2 + RDS. Same code, different tracking URI.
