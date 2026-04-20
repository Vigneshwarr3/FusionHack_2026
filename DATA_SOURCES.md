# AquiferWatch Data Sources

A complete inventory of every external data source ingested into this project, the actual data on disk, how each source feeds the baseline/scenarios/UI, and known limitations.

**Audit rule:** Every number displayed in the app must trace back to a row in this document. If you can't justify a cell in the dashboard from a source listed here, it's a bug.

---

## One-page summary

| # | Source | What it gives us | Rows | Baseline role |
|---|---|---|---|---|
| 1 | USGS HPA boundary (DS-543) | 8-state aquifer polygon | 199 polygons | Map extent, county-HPA overlap filter |
| 2 | USGS NWIS monitoring wells | HPA well inventory | 48,064 wells | Well-count per county, gap-fill where state data absent |
| 3 | USGS Water Data OGC API | Field water-level measurements | ~1,650 rows (sparse, 7% hit) | Gap-fill water levels, decline slopes |
| 4 | USGS Water Use 2015 | County irrigation withdrawals | 715 HPA counties | **Pumping validation** (found our inferred is 0.52× reported) |
| 5 | USDA NASS Census | County × crop irrigated acres | 8,260 county-crop rows | **Spine of baseline** — per-county crop mix |
| 6 | USDA IWMS 2023 | Water applied per acre by crop/state | 45 rows | Per-crop water rate in inferred pumping |
| 7 | USDA ERS crop budgets | $ gross value per acre (national) | 5 crops | `REVENUE_PER_ACRE` constants → ag-value delta |
| 8 | EPA eGRID 2022 | State grid CO₂ intensity | 52 states | Pumping → CO₂ conversion |
| 9 | KGS WIMAS rights inventory | KS irrigation water rights | 60,694 rights | KS irrigation operation counts per county |
| 10 | KGS WIMAS state annual summary | KS irrigation totals 1955–2024 | 70 years | Demo time-scrubber anchor |
| 11 | KGS Master Well Inventory | All KS wells + well_depth + join keys | 374,995 wells | Depth metadata for thickness computation |
| 12 | KGS HPA bedrock wells | KS bedrock-elevation wells | 37,026 wells | Per-county max-possible aquifer thickness |
| 13 | **KGS WIZARD** | **KS water-level time series** | **8,061 sites / 601k measurements** | **89-county thickness + decline** |
| 14 | TX TWDB GWDB | TX Ogallala wells + water levels | 22,546 wells / 238k measurements | TX thickness + decline |
| 15 | NE DEE registered wells | NE wells + static water levels | 265,757 wells | NE thickness (via TotalDepth − StaticWaterLevel) |
| 16 | **USGS NGWMN portal** | **Multi-source HPA water levels** | **10,140 sites / 1.68M measurements** | **184 non-KS counties thickness + decline** |

**Baseline coverage as of 2026-04-20:** 248 of 606 HPA counties (41%) have real per-county thickness + decline from measurements. Remaining 358 use HPA-median fallback (30 m thickness, −0.30 m/yr decline), flagged as `data_quality="modeled_low"` per spec §4.

By state:
- NE: 93/93 (100%)
- KS: 85/92 (92%)
- CO: 12/57 (21%)
- TX: 41/213 (19%)
- WY: 1/7 (14%)
- OK: 9/67 (13%)
- SD: 5/50 (10%)
- NM: 2/27 (7%)

---

## Detailed source-by-source

### 1. USGS High Plains Aquifer Boundary (DS-543)

- **ScienceBase item:** `6314061bd34e36012efa397b`
- **Ingest:** [`pipeline/hpa_footprint.py`](aquiferwatch/pipeline/hpa_footprint.py)
- **Raw:** `data/raw/usgs/ds543.zip` (1 MB shapefile)
- **Processed:** `data/processed/hpa_boundary.parquet` (199 polygons, EPSG:4326)
- **Used for:** Map base layer (GeoJsonLayer in deck.gl), county-HPA-overlap filter when selecting which US counties belong in the baseline.
- **Limitations:** Polygons don't carry per-region thickness density (outcrop/little/some/none are in the KGS HPA extent dataset, source #12, not this one). Treat as binary membership only.

### 2. USGS NWIS Site Inventory

- **Endpoint:** `https://waterservices.usgs.gov/nwis/site/?aquiferCd=N100HGHPLN`
- **Ingest:** [`pipeline/usgs_wells.py`](aquiferwatch/pipeline/usgs_wells.py) `fetch_all_sites()`
- **Raw:** `data/raw/usgs/sites_{state}.rdb`
- **Processed:** `data/processed/usgs_wells.parquet` (48,064 wells × 43 columns)
- **Used for:** Well density per county (feature for imputation), altitude/depth gap-fill for counties without state-agency data.
- **Limitations:** USGS monitoring wells only — does NOT include private irrigation wells. The bigger registered-wells counts live in NE DEE (#15) and KGS Master Inventory (#11).

### 3. USGS Water Data OGC API (field-measurements)

- **Endpoint:** `https://api.waterdata.usgs.gov/ogcapi/v0/collections/field-measurements/items`
- **Parameter:** `72019` (Depth to water level, feet below land surface)
- **Ingest:** [`pipeline/usgs_wells.py`](aquiferwatch/pipeline/usgs_wells.py) `fetch_gwlevels()`
- **Orchestrator:** [`scripts/pull_gwlevels_targeted.py`](scripts/pull_gwlevels_targeted.py)
- **Processed:** `data/processed/usgs_gwlevels.parquet`
- **Used for:** Gap-filling water-level data where state sources don't cover.
- **Limitations:** Very sparse — only ~7% of HPA wells have measurements in this collection (we tested 1,300 wells, 96 returned data over ~5 hours). **Superseded by NGWMN (#16)** which aggregates the same underlying data plus state partner networks. Kept in the pipeline for incremental updates post-MVP.
- **Migration note:** Legacy `nwis/gwlevels/` was decommissioned Fall 2025. `dataretrieval-python` v1.1.3 is the recommended replacement for programmatic pulls (same OGC endpoint but with bulk-site queries + API-key support).

### 4. USGS Estimated Use of Water in the US, 2015

- **ScienceBase item:** `5af3311be4b0da30c1b245d8` (`usco2015v2.0.csv`)
- **Ingest:** [`pipeline/usgs_water_use.py`](aquiferwatch/pipeline/usgs_water_use.py)
- **Processed:** `data/processed/usgs_water_use_2015.parquet` (715 HPA-state county rows)
- **Used for:** Independent validation of our inferred pumping. Carried as `pumping_af_yr_usgs2015` column in the baseline alongside `pumping_af_yr_inferred`.
- **Key finding documented in [limitations.md §4b](docs/limitations.md):** Our NASS × IWMS inferred pumping is **0.52× USGS reported** (9.5M AF/yr ours vs. 18.1M AF/yr USGS, correlation 0.70). Ranking of counties is preserved; absolute magnitude is half. Consequence: scenarios' percentage deltas unaffected; absolute CO₂-Mt numbers roughly 2× understated.
- **Limitations:** 2015-only. USGS publishes quinquennially; 2020 hasn't been released yet.

### 5. USDA NASS Census of Agriculture — County × Crop Irrigated Acres

- **Endpoint:** QuickStats API (reuses `QUICKSTATS_API_KEY` from parent project)
- **Query:** `source_desc=CENSUS`, `prodn_practice_desc=IRRIGATED`, `statisticcat_desc=AREA HARVESTED`, years 2017 + 2022
- **Ingest:** [`pipeline/nass_crops.py`](aquiferwatch/pipeline/nass_crops.py)
- **Processed:** `data/processed/nass_irrigated_acres.parquet` (8,260 rows)
- **Used for:** **The spine of the baseline.** Per-county irrigated acres by crop drive the crop_mix × water_rate → inferred pumping, and the crop × revenue_per_acre → ag_value. Scenario crop-shift math operates on these columns.
- **Limitations:** Census only (every 5 years); annual SURVEY doesn't split irrigation at county level. We use 2022 values as current-day baseline. Confidentiality suppression blanks some state-crop-county cells (ignored).

### 6. USDA NASS IWMS 2023 — Water Applied per Acre

- **Endpoint:** QuickStats API, `statisticcat_desc=WATER APPLIED`, `unit_desc=ACRE FEET / ACRE`
- **Ingest:** [`pipeline/iwms_water.py`](aquiferwatch/pipeline/iwms_water.py)
- **Processed:** `data/processed/iwms_water_per_acre.parquet` (45 rows, partial)
- **Used for:** Per-crop per-state water application rate. Joins to NASS acres to produce inferred pumping.
- **Limitations:** NASS confidentiality blanks many smaller-state-crop combinations. Where missing, we fall back to HPA-average `WATER_PER_ACRE` constants in [scenarios.py](aquiferwatch/analytics/scenarios.py). Combined with source #5 this produces ~half of USGS-reported pumping (see #4).

### 7. USDA ERS Commodity Costs & Returns

- **URL:** `https://www.ers.usda.gov/media/{id}/{crop}.xlsx`
- **Ingest:** [`pipeline/ers_budgets.py`](aquiferwatch/pipeline/ers_budgets.py) (reads `Data sheet (machine readable)` tab)
- **Processed:** `data/processed/ers_revenue_per_acre.parquet` (5 crops, 2024)
- **Used for:** `REVENUE_PER_ACRE` constants in [scenarios.py](aquiferwatch/analytics/scenarios.py) — corn $757, cotton $686, soybeans $544, wheat $287, sorghum $279. Alfalfa held at HPA extension-service median ($1,100) since ERS doesn't publish alfalfa budgets.
- **Limitations:** National totals, not HPA-specific. Irrigated corn in the HPA yields higher than the national mean so these values slightly under-count ag value for the high-yield counties. Documented in `docs/limitations.md`.

### 8. EPA eGRID 2022 — State Grid Carbon Intensity

- **URL:** `https://www.epa.gov/system/files/documents/2024-01/egrid2022_data.xlsx`
- **Ingest:** [`pipeline/eia_grid.py`](aquiferwatch/pipeline/eia_grid.py)
- **Processed:** `data/processed/eia_grid_intensity.parquet` (52 states)
- **Used for:** State-level CO₂-per-kWh factor in the scenario CO₂-delta formula. WY 0.82 kg/kWh (coal-heavy), SD 0.15 kg/kWh (hydro+wind).
- **Limitations:** 2022 annual average. Grid is decarbonizing ~3%/year, so using static 2022 overstates future-year CO₂ deltas slightly.

### 9. KGS WIMAS Rights Inventory

- **Source:** KGS WIMAS query portal (https://www.kgs.ku.edu/Magellan/WaterWell/ → WIMAS Water Rights Query)
- **File:** `data/raw/kgs_wimas/wimas_20260418974.txt` (pipe-delimited, 60,694 rows)
- **Ingest:** [`pipeline/kgs_wimas.py`](aquiferwatch/pipeline/kgs_wimas.py) `_process_rights()`
- **Processed:** `data/processed/kgs_wimas_rights.parquet`
- **Used for:** Count of active irrigation rights per KS county (35,018 across 40 HPA counties). Feature for the TX extraction imputation model when we train it.
- **Limitations:** **This is water RIGHTS, not actual pumpage.** Shows who is *allowed* to extract, not who did and how much. The per-right annual pumpage is only available via Open Records Request (filed 2026-04-18, awaiting response).

### 10. KGS WIMAS State Annual Summary

- **File:** `data/raw/kgs_wimas/wusesum_20260418705.txt`
- **Processed:** `data/processed/kgs_wimas_state_annual.parquet` (70 years, 1955–2024)
- **Used for:** KS state-wide total irrigation use by year. Anchors the demo time-scrubber ("watch KS pumping grow 1955 → peak → stabilize"). 2020 total: 2.99 M AF, 2.9 M acres irrigated.
- **Limitations:** State-wide only, not per-county.

### 11. KGS Master Well Inventory

- **Source:** Kansas Geoportal (DASC), Master Well Inventory dataset
- **File:** `data/raw/kgs_master/master_well_inventory.csv` (88 MB)
- **Ingest:** [`pipeline/kgs_master.py`](aquiferwatch/pipeline/kgs_master.py)
- **Processed:** `data/processed/kgs_master_wells.parquet` (374,995 wells)
- **Used for:** Canonical KS well metadata — `WELL_DEPTH` (91% populated), lat/lon (99.9%), `WIZARD_USGS_ID` join key (52,967 wells). The well-depth data enables (`WELL_DEPTH − depth_to_water`) → saturated thickness when combined with WIZARD (#13).
- **Limitations:** KS only. Well depth is at completion; the aquifer may have dropped since drilling for older wells.

### 12. KGS HPA Bedrock Wells

- **Source:** Kansas Geoportal, High Plains Aquifer dataset → bedrock_wells attribute table
- **File:** `data/raw/kgs_highplains_aquifer/High_Plains_Aquifer_bedrock_wells.csv`
- **Ingest:** [`pipeline/kgs_hpa.py`](aquiferwatch/pipeline/kgs_hpa.py)
- **Processed:** `data/processed/kgs_bedrock_wells.parquet` (37,026 wells) + `kgs_county_bedrock.parquet` (70 counties)
- **Used for:** Per-county implied max aquifer thickness (surface elevation − bedrock elevation). Used as a bedrock-based 50%-fill fallback estimate for KS counties without water-level data. Validates published HPA thicknesses — Stevens 161m, Haskell 157m match Deines 2019.
- **Limitations:** Doesn't give current saturated thickness, only the physical housing capacity. The 50% fill factor is a Deines 2019 HPA average, not per-county.

### 13. KGS WIZARD Water-Level Database (★ authoritative Kansas source)

- **Source:** KGS WIZARD query portal (https://www.kgs.ku.edu/Magellan/WaterLevels/)
- **Files:** `data/raw/kgs_wimas/WIZARD/sites*.csv` + `wlevel*.csv` (8 paired exports)
- **Ingest:** [`pipeline/kgs_wizard.py`](aquiferwatch/pipeline/kgs_wizard.py)
- **Processed:** `data/processed/kgs_wizard_sites.parquet` (8,061 wells), `kgs_wizard_levels.parquet` (601,454 measurements), `kgs_wizard_county_thickness.parquet` (89 counties)
- **Used for:** **The top-priority KS thickness and decline source in the baseline composer.** Wins over every other KS pipeline where it has coverage.
- **Limitations:** KS only. Measurements concentrated Jan-Feb (the KGS/DWR cooperative winter-mass measurement season), so mid-year dynamics aren't captured — but annual-scale decline slopes are clean.

### 14. Texas Water Development Board GWDB

- **Source:** TWDB Groundwater Database full export (https://www.twdb.texas.gov/groundwater/data/)
- **Files:** `data/raw/twdb/GWDBDownload.zip` (81 MB) + `TWDB_Groundwater.zip` (shapefile)
- **Ingest:** [`pipeline/tx_twdb.py`](aquiferwatch/pipeline/tx_twdb.py)
- **Processed:** `twdb_wells.parquet` (22,546 Ogallala wells), `twdb_water_levels.parquet` (238k measurements), `twdb_county_thickness.parquet` (40 counties), `twdb_well_points.parquet` (142k statewide points)
- **Used for:** TX per-county thickness + decline. 35 of 37 TX HPA counties show active depletion. Dallam −0.72 m/yr matches the spec §9 "cautionary tale" narrative.
- **Limitations:** Decline method requires per-well slope of `WaterElevation` (not cross-wells median thickness — that produces spurious positive slopes from sampling bias; see the commit log of [tx_twdb.py](aquiferwatch/pipeline/tx_twdb.py) for the debug).

### 15. Nebraska DEE Registered Wells

- **Source:** Nebraska DEE → Groundwater → Registered Groundwater Wells Data → GIS Well Data CSV
- **File:** `data/raw/ne_dnr/Groundwater_Wells_DWEE.csv` (90 MB)
- **Ingest:** [`pipeline/ne_dnr.py`](aquiferwatch/pipeline/ne_dnr.py)
- **Processed:** `ne_dnr_wells.parquet` (265,757 wells), `ne_dnr_county_summary.parquet` (93 counties)
- **Used for:** NE per-county thickness (via median recent-completion `TotalDepth − StaticWaterLevel`). 100% of NE HPA counties covered. Anchors the spec §9 "Nebraska exception" story — median decline −0.10 m/yr vs HPA-wide −0.30.
- **Limitations:** `StaticWaterLevel` is at well completion, not today. For counties with lots of pre-2000 wells, our "current" thickness trails reality. Partially mitigated by filtering to "past 25 years" completions.

### 16. USGS NGWMN — National Ground-Water Monitoring Network

- **Source:** NGWMN data portal (https://cida.usgs.gov/ngwmn/) → Principal Aquifer: High Plains → download per-county CSVs
- **Files:** `data/raw/ngwmn/{STATE}/data*.zip` (134 zips across 7 state folders)
- **Ingest:** [`pipeline/ngwmn.py`](aquiferwatch/pipeline/ngwmn.py) — recursively scans subdirs, reads `SITE_INFO.csv` per zip, auto-routes by StateCd+CountyCd
- **Processed:** `ngwmn_sites.parquet` (10,140 unique wells across 201 counties), `ngwmn_water_levels.parquet` (1.68M measurements), `ngwmn_county_thickness.parquet` (184 counties)
- **Used for:** **The top-priority non-KS thickness and decline source.** Aggregates USGS NWIS + KGS + TX DWR + NE DNR + other state partner networks into one consistent schema.
- **Limitations:** The NGWMN portal caps each download at ~100 sites, so users drop many `data(*).zip` files. Parser handles this automatically — just dump zips into `data/raw/ngwmn/`. Some counties genuinely lack monitoring infrastructure (visible as blank regions in the NGWMN map); those fall through to the bedrock or state fallbacks.

---

## Baseline composition priority

When multiple sources have data for the same county, [`pipeline/compose_baseline.py`](aquiferwatch/pipeline/compose_baseline.py) applies them in this order (first-applied wins):

1. **KGS WIZARD** (KS only) → thickness + decline
2. **NGWMN** (any state) → thickness + decline
3. **TWDB** (TX only) → thickness + decline gap-fill
4. **KGS bedrock × 0.5 fill** (KS only) → thickness estimate for remaining KS counties
5. **KGS old sparse USGS** (KS only) → decline estimate for remaining KS counties
6. **NE DEE override** (NE only) → thickness for NE counties not yet filled
7. **HPA-median fallback** → 30m thickness, −0.30 m/yr decline (for counties with no measurements)

Pumping is always computed as Σ(per-crop acres × IWMS rate) so scenarios are internally consistent. USGS 2015 reported values live in `pumping_af_yr_usgs2015` for audit/display but don't drive scenario math.

---

## Data quality flags

Each baseline row carries a `data_quality` enum per spec §4:
- `metered` — directly observed pumpage (currently empty; filled by KS ORR when it arrives)
- `modeled_high` — real thickness + decline from WIZARD/NGWMN/TWDB/NE DEE
- `modeled_low` — fallback thickness or decline
- `no_data` — reserved for counties with neither crop nor water data

---

## Awaiting ingestion (not blocking demo)

| Source | Status | Gives us |
|---|---|---|
| Kansas Open Records Request (county annual pumpage 2000–2024) | Filed 2026-04-18; 1–5 business days | Training labels for TX extraction imputation; real KS time-scrubber |
| NE NRD annual use reports | Deferred (23 districts, each different) | "Nebraska exception" story embellishment |
| PRISM precipitation normals | Deferred (only needed for imputation training) | Feature for TX imputation model |

## Deferred / skipped

- **NRCS EQIP / Ogallala Aquifer Initiative** — policy overlay, 90% visual, no scenario-math impact
- **EIA electricity prices** — only matters for a price-elasticity scenario we don't have
- **Deines 2019 data** — it's a methodology reference, formulas are already in our scenarios

---

*Last updated 2026-04-20. See the git log of `data/processed/*.parquet` and `aquiferwatch/pipeline/*.py` for the most recent numbers if this file drifts.*
