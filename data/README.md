# data/

Local-only data workspace. Contents of `raw/` and `interim/` are gitignored; `processed/` holds publication-ready parquet and is mirrored to S3 (`s3://usda-analysis-datasets/aquiferwatch/processed/`).

- `raw/` — original downloads, one subdirectory per source (`usgs_wells/`, `kgs_wimas/`, ...). Never modify; re-download if corrupted.
- `interim/` — cleaned but pre-join county tables.
- `processed/` — publication-ready per-county parquet for the API + the public download feature.
