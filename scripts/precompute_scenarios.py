"""Precompute scenario snapshots so the frontend can toggle scenarios
without a live backend call.

Runs each of the 5 named scenarios (status_quo, ks_lema_aquifer_wide,
drip_transition, corn_reduction_25, no_ag_below_9m) against the current
baseline and dumps one JSON per scenario:

    data/processed/web/scenarios/<id>.json
    data/processed/web/scenarios/_index.json   ← list of available scenarios

Each scenario JSON is a ScenarioResult (schemas.py) serialized to plain
JSON, plus a `computed_at` timestamp and the baseline version.

Usage
-----
    poetry run python scripts/precompute_scenarios.py
    poetry run python scripts/precompute_scenarios.py --upload
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd

from aquiferwatch.analytics.baseline import load_baseline
from aquiferwatch.analytics.scenarios import run_scenario
from aquiferwatch.config import DATA_DIR
from aquiferwatch.schemas import ScenarioID

REPO_ROOT = DATA_DIR.parent
WEB_DIR = DATA_DIR / "processed" / "web"
SCENARIO_DIR = WEB_DIR / "scenarios"

BUCKET = os.getenv("S3_BUCKET", "usda-analysis-datasets")
S3_PREFIX = "aquiferwatch/web"

SCENARIO_META = {
    ScenarioID.STATUS_QUO: {
        "display_name": "Status Quo",
        "description": "Current extraction + crop mix continues.",
    },
    ScenarioID.KS_LEMA_AQUIFER_WIDE: {
        "display_name": "Kansas LEMA, aquifer-wide",
        "description": "Sheridan-6 rules applied to every HPA county.",
        "source": "Basso et al. 2025",
    },
    ScenarioID.DRIP_TRANSITION: {
        "display_name": "Drip irrigation transition",
        "description": "Center-pivot + flood → drip over 10 years.",
    },
    ScenarioID.CORN_REDUCTION_25: {
        "display_name": "25% corn reduction",
        "description": "Corn acreage drops 25%; reallocated to sorghum/wheat/dryland.",
    },
    ScenarioID.NO_AG_BELOW_9M: {
        "display_name": "No ag below 9m threshold",
        "description": "Counties below 9m saturated thickness stop pumping.",
    },
}


def build() -> list[Path]:
    baseline = load_baseline()
    print(f"baseline: {len(baseline)} counties")

    SCENARIO_DIR.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    index_entries = []

    for scenario_id, meta in SCENARIO_META.items():
        print(f"  running {scenario_id.value}…")
        result = run_scenario(scenario_id, baseline, run_id="precomputed")
        payload = json.loads(result.model_dump_json())
        payload["display_name"] = meta["display_name"]
        payload["description"] = meta["description"]
        if "source" in meta:
            payload["source"] = meta["source"]

        out = SCENARIO_DIR / f"{scenario_id.value}.json"
        out.write_text(json.dumps(payload, separators=(",", ":")))
        size_kb = out.stat().st_size / 1024
        print(f"    wrote {out.relative_to(REPO_ROOT)}  ({size_kb:.1f} KB)  "
              f"lifespan_ext={result.aquifer_lifespan_extension_years:+.1f}y  "
              f"ag_delta=${result.cumulative_ag_production_delta_usd_b:+.2f}B")
        written.append(out)
        index_entries.append({
            "id": scenario_id.value,
            "display_name": meta["display_name"],
            "description": meta["description"],
            "source": meta.get("source"),
            "url": f"scenarios/{scenario_id.value}.json",
            "headline": {
                "aquifer_lifespan_extension_years": result.aquifer_lifespan_extension_years,
                "cumulative_ag_production_delta_usd_b": result.cumulative_ag_production_delta_usd_b,
                "rural_employment_delta_pct": result.rural_employment_delta_pct,
                "embedded_co2_delta_mt": result.embedded_co2_delta_mt,
            },
        })

    index = {
        "version": pd.Timestamp.utcnow().isoformat(),
        "baseline_counties": len(baseline),
        "scenarios": index_entries,
    }
    index_path = SCENARIO_DIR / "_index.json"
    index_path.write_text(json.dumps(index, indent=2))
    print(f"wrote index {index_path.relative_to(REPO_ROOT)}")
    written.append(index_path)
    return written


def upload(paths: list[Path]) -> None:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-2"))
    for local in paths:
        # Preserve the /scenarios/ subfolder in S3 too.
        key = f"{S3_PREFIX}/scenarios/{local.name}"
        print(f"upload {local.name} -> s3://{BUCKET}/{key}")
        try:
            s3.upload_file(
                str(local), BUCKET, key,
                ExtraArgs={"ContentType": "application/json"},
            )
        except (BotoCoreError, ClientError) as e:
            sys.exit(f"upload failed: {e}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--upload", action="store_true")
    args = p.parse_args()

    paths = build()
    if args.upload:
        upload(paths)


if __name__ == "__main__":
    main()
