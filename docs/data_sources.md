# Data sources

See [`../aquiferwatch_spec.md`](../aquiferwatch_spec.md) §4 for the canonical list with commentary. This file tracks the *operational* state — endpoint, auth, refresh cadence, owner.

| Source | Endpoint / path | Auth | Refresh | Owner | Status |
|---|---|---|---|---|---|
| USGS High Plains Aquifer shapefile | ScienceBase item 5b0cf8b7e4b06a6e52631f36 | none | one-time | teammate | not started |
| USGS Water Services (wells) | `https://waterservices.usgs.gov/nwis/` | User-Agent | daily (historical one-shot) | teammate | not started |
| USDA NASS QuickStats | REUSE parent project | `QUICKSTATS_API_KEY` | monthly (parent cron) | Raj | reused |
| USDA IWMS 2023 | ers.usda.gov | none | static | paired | not started |
| KGS WIMAS | kgs.ku.edu/Magellan/WaterWell/ | none | annual | teammate | not started |
| TX TWDB | twdb.texas.gov/groundwater/data/ | none | annual | teammate | not started |
| NE DNR | dnr.nebraska.gov | none | annual | teammate | not started |
| NRCS EQIP / OAI | nrcs.usda.gov | none | annual | Raj | not started |
| ERS crop budgets | ers.usda.gov | none | annual | paired | not started |
| EIA grid intensity | eia.gov/electricity/data/eia923 | none | annual | Raj | not started |
| NOAA climate normals | REUSE parent project | `NOAA_API_KEY` | static | Raj | reused |

**Reused** means: no duplicate ingestion here; we either import the table from the parent RDS or invoke the parent pipeline and read its S3 parquet.

**Owner** columns reflect spec §7 split (Raj = frontend + FastAPI + methodology; teammate = Python/ML/DE; paired = Day 0 contracts + economics + QA).
