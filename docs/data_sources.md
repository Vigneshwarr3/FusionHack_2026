# Data sources

See [`../aquiferwatch_spec.md`](../aquiferwatch_spec.md) §4 for the canonical list with commentary. This file tracks the *operational* state — endpoint, auth, refresh cadence, owner.

| Source | Endpoint / path | Auth | Refresh | Status | Rows |
|---|---|---|---|---|---|
| USGS HPA footprint | ScienceBase DS-543 | none | static | ✓ ingested | 199 polygons |
| USGS monitoring wells | `waterservices.usgs.gov/nwis/site/` | User-Agent | daily | ✓ ingested | 48,064 wells |
| USGS water levels | `api.waterdata.usgs.gov/ogcapi/v0/` field-measurements (param 72019) | User-Agent | weekly | ⚙ partial (rate-limited; sampled) | ~ |
| USDA NASS irrigated acres | QuickStats CENSUS + `prodn_practice_desc=IRRIGATED` | `QUICKSTATS_API_KEY` (reused) | 5-yearly census | ✓ ingested | 8,260 |
| USDA IWMS 2023 water applied | QuickStats CENSUS `WATER APPLIED` / `ACRE FEET / ACRE` | `QUICKSTATS_API_KEY` | annual | ✓ ingested (partial — NASS confidentiality blanks some cells) | 45 |
| USDA ERS crop budgets | `ers.usda.gov/media/{id}/{crop}.xlsx` | none | annual | ✓ ingested | 5 crops |
| EIA grid intensity | `epa.gov/…/egrid2022_data.xlsx` | none | annual | ✓ ingested | 52 states |
| KGS WIMAS | `hercules.kgs.ku.edu` CFM queries | none | annual | ✗ manual download (see module docstring) | — |
| TX TWDB | `twdb.texas.gov/groundwater/data/` | none | annual | ✗ manual download | — |
| NE DNR | `dnrdata.dnr.ne.gov/wellssql/` | none | annual | ✗ manual download | — |
| NRCS EQIP / OAI | `nrcs.usda.gov` | none | annual | ✗ deferred (post-accuracy-gate) | — |
| NOAA climate normals | REUSE parent project | `NOAA_API_KEY` | static | ✓ reused | — |

**Programmatic-access caveats.** Kansas, Texas, and Nebraska state-board data each require either form-based CFM interfaces, behind-the-firewall ArcGIS services, or PDF reports. The state-ingest modules (`pipeline/kgs_wimas.py`, `tx_twdb.py`, `ne_dnr.py`) each document the manual CSV drop-in path; parsers are ready once files land in `data/raw/<source>/`.

**Reused** means: no duplicate ingestion here; we either import the table from the parent RDS or invoke the parent pipeline and read its S3 parquet.

**Owner** columns reflect spec §7 split (Raj = frontend + FastAPI + methodology; teammate = Python/ML/DE; paired = Day 0 contracts + economics + QA).
