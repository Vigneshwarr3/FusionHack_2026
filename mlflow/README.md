# MLflow for AquiferWatch

Goal: Raj and teammate can see each other's runs, compare metrics side-by-side, and reuse each other's registered models — without either of us thinking about infrastructure after Day 0.

## Day 0 — local SQLite (you're here)

Works immediately, no shared infra needed. `MLFLOW_TRACKING_URI` defaults to `sqlite:///mlflow.db` in `.env.example`. Start the UI:

```bash
poetry run mlflow ui --backend-store-uri sqlite:///mlflow.db --port 5000
```

Open http://localhost:5000. This is fine for the first day or two while each of us explores.

Limitation: runs are only visible on the machine that created them. Switch to team mode as soon as you want to compare results across machines.

## Team mode — shared backend on existing infra

We reuse what already exists in Agricultural_Data_Analysis: the `ag-dashboard` RDS instance + the `usda-analysis-datasets` S3 bucket. No new infra, no new bill.

**Step 1 — create the MLflow metadata DB on the existing RDS** (one-time, run from any machine that can reach the RDS):

```sql
CREATE DATABASE mlflow;
CREATE USER mlflow_user WITH PASSWORD '<choose-one>';
GRANT ALL PRIVILEGES ON DATABASE mlflow TO mlflow_user;
```

**Step 2 — host the MLflow server**. Options, easiest first:

- **Option A (recommended for hackathon): systemd on the existing EC2.** The parent project already runs `ag-prediction.service` on EC2. Add a sibling `mlflow.service`:

  ```ini
  [Unit]
  Description=MLflow tracking server
  After=network.target

  [Service]
  User=ubuntu
  EnvironmentFile=/home/ubuntu/aquifer-watch/.env
  ExecStart=/home/ubuntu/aquifer-watch/.venv/bin/mlflow server \
      --host 0.0.0.0 --port 5000 \
      --backend-store-uri postgresql://mlflow_user:PASSWORD@${DATABASE_ENDPOINT}:5432/mlflow \
      --default-artifact-root s3://usda-analysis-datasets/aquiferwatch/mlartifacts
  Restart=on-failure

  [Install]
  WantedBy=multi-user.target
  ```

  Expose port 5000 in the EC2 security group to our two IPs only. Swap over each person's `.env`:

  ```
  MLFLOW_TRACKING_URI=http://<ec2-public-dns>:5000
  MLFLOW_ARTIFACT_LOCATION=s3://usda-analysis-datasets/aquiferwatch/mlartifacts
  ```

- **Option B: Databricks Community Edition.** Free, managed. Only downside is registration friction and an external dependency.

- **Option C: Railway.** One-click MLflow deploy, $5/mo. Use if EC2 access is a blocker.

## Standard tags

`aquiferwatch.mlflow_utils.start_run` tags every run with:

- `team_member` — from `AQW_TEAM_MEMBER` env var (each of us sets our own)
- `module` — `depletion` | `extraction_imputation` | `scenarios` | `saturated_thickness` | ...
- `scenario` — scenario id when applicable, else `n/a`
- `git_sha` — short SHA for reproducibility

Filter in the UI to see "all of teammate's depletion runs" or "every LEMA scenario run this week" with a single tag query.

## Model registry convention

Register a model in MLflow once it has cleared its acceptance gate (see `docs/integration.md` §order of operations). Naming: `aquiferwatch_<module>`. Stages: `Staging` → `Production` via PR-driven promotion.
