# Methodology

*This document is the traceability spine for the tool. Every on-screen number must trace to a formula and a cited source here.*

## 1. Aquifer footprint

Counties are classified as "HPA counties" if their polygon overlaps the USGS High Plains Aquifer boundary (source: USGS High Plains Aquifer shapefile, most recent release). For counties partially overlapping, metrics are computed on the overlap area and flagged with `aquifer_overlap_pct`.

## 2. Saturated thickness

**Observation source:** USGS monitoring well network + Kansas Geological Survey WIMAS wells + Texas Water Development Board wells + Nebraska DNR wells.

**Interpolation:** *[Day 1 — fill in]*. Baseline: IDW. Stretch: ordinary kriging with a spherical variogram fit per state. Validate against Deines et al. 2019 Kansas results (target: per-county R² ≥ 0.80).

## 3. Annual decline and recharge

**Annual decline** per county is the slope of saturated thickness over the last 10 available years. **Recharge** uses published long-term averages per HUC-8 from the USGS Water Use assessments; constant across the projection horizon per spec §3 ("counterfactuals assume static climate").

## 4. Years-until-uneconomic

Threshold: 9 m saturated thickness (below which center-pivot pumping becomes uneconomic at typical Ogallala depths). Formula: `max((thickness − 9) / annual_decline, 0)`. Per-county pumping cost assumptions documented in `docs/limitations.md` §4.

## 5. Crop water footprint

**Water applied per acre** per crop per state comes from the USDA Irrigation and Water Management Survey 2023. Per-county irrigated acres by crop from NASS QuickStats. County × crop water use = irrigated acres × water applied per acre.

**$ per acre-foot** = (gross value from NASS QuickStats × per-county price adjustment from ERS) / water applied. Full derivation per crop in `docs/economics.md`.

## 6. Texas extraction imputation

Texas lacks metered extraction county-wide (rule of capture). We train a LightGBM model on Kansas + Nebraska metered pumpage as the training set, with features (irrigated acres by crop, crop mix, saturated thickness, well density, PRISM precip, ERS price signals), then predict per-Texas-county extraction. Uncertainty bands from quantile regression (p10/p50/p90), displayed on map as a data-quality ring. Validation: 5-fold spatial CV on Kansas-only, target MAE ≤ 20% of median observed pumpage.

## 7. Scenario math

See [scenarios.md](scenarios.md) for the full dispatch table and per-scenario formula.

## 8. Emissions

Embedded CO₂ of pumping = pumping volume (acre-ft) × kWh per acre-foot (from USDA NRCS irrigation energy budgets, per irrigation method) × state grid intensity (kg CO₂/kWh from EIA eGRID).

## 9. Known limitations

See [limitations.md](limitations.md). Every limitation must surface in the UI as either a data-quality indicator on the map or a footnote in the scenario panel.
