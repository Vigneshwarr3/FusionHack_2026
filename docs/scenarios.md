# Scenarios

Implementation: `aquiferwatch/analytics/scenarios.py`. Tests: `tests/test_scenarios.py`.

Every scenario is a **deterministic** function of `(baseline, params) → ScenarioResult`. The math below is the canonical reference; if code and docs diverge, the test suite is the tie-breaker.

---

## Shared inputs

### Baseline schema

The baseline is a single row per county with the columns documented in the docstring at the top of `scenarios.py`. Key fields:

- `saturated_thickness_m` — current aquifer thickness
- `annual_decline_m` — **signed** (negative = depleting, per `schemas.AquiferSection` convention)
- `pumping_af_yr` — current annual extraction (acre-feet)
- `acres_{corn,soybeans,sorghum,wheat,cotton,alfalfa}` — per-crop irrigated acres
- `irr_{center_pivot,flood,drip,dryland}` — shares of irrigated acres, sum ~1
- `kwh_per_af_pumped` × `grid_intensity_kg_per_kwh` — electricity carbon intensity of pumping

### Shared constants (`scenarios.py`)

| Name | Value | Source |
|---|---|---|
| `WATER_PER_ACRE["corn"]` | 1.40 ac-ft/ac/yr | USDA IWMS 2023 (HPA avg) |
| `WATER_PER_ACRE["soybeans"]` | 1.00 | IWMS 2023 |
| `WATER_PER_ACRE["sorghum"]` | 0.80 | IWMS 2023 |
| `WATER_PER_ACRE["wheat"]` | 0.70 | IWMS 2023 |
| `WATER_PER_ACRE["cotton"]` | 1.10 | IWMS 2023 |
| `WATER_PER_ACRE["alfalfa"]` | 1.80 | IWMS 2023 |
| `REVENUE_PER_ACRE` | See code | USDA ERS county-level budgets, HPA median |
| `METHOD_EFFICIENCY["center_pivot"]` | 1.00 | NRCS IWM reference |
| `METHOD_EFFICIENCY["flood"]` | 1.15 | NRCS IWM reference |
| `METHOD_EFFICIENCY["drip"]` | 0.65 | NRCS IWM reference (30–40% savings) |
| `METHOD_EFFICIENCY["dryland"]` | 0.00 | No irrigation |
| `THICKNESS_THRESHOLD_M` | 9.0 m | Deines et al. 2019 |
| `EMPLOYMENT_PER_AG_USD` | 0.021 FTE per $1000 | USDA ERS IMPLAN-style multiplier |

### Shared delta formulas

Computed by `_apply_and_summarize` for every scenario:

- `pump_ratio = pumping_mod / pumping_base`
- `new_annual_decline = annual_decline_base × pump_ratio`
- `years_until_uneconomic = (thickness - 9) / |annual_decline|`, clipped at `[0, 1000]`. If `annual_decline ≥ 0` (stable or rising), lifespan is 1000.
- `years_delta = years_mod - years_base`
- `co2_delta_mt = (pumping_mod - pumping_base) × kwh_per_af × grid_intensity / 1000`
- `employment_delta_fte = ag_value_delta × 0.021 / 1000`
- **Aquifer-level** lifespan extension = pumping-weighted mean of `years_delta` across HPA counties (heavy pumpers weighted more).

---

## Scenarios

### 1. `status_quo`

Identity. Returns zero deltas. Serves as the comparison anchor in the UI.

### 2. `ks_lema_aquifer_wide`

Apply Sheridan-6 LEMA rules to every HPA county.

**Assumed deltas** (Basso et al. 2025):
- Corn acreage drops 15%; shift goes to sorghum (60% of shift) and wheat (40% of shift).
- Pumping reduces 27% (LEMA program target).

**Math:**
```
acres_corn       → acres_corn × (1 - 0.15)
acres_sorghum    += 0.15 × acres_corn × 0.60
acres_wheat      += 0.15 × acres_corn × 0.40
pumping_af_yr    → pumping_af_yr × (1 - 0.27)
ag_value_usd     = recomputed from new crop mix × REVENUE_PER_ACRE
```

Downstream deltas follow from the shared engine.

### 3. `drip_transition`

Endstate of a 10-year transition: all center-pivot + flood acres → drip.

**Math:**
```
new_irr_drip   = irr_drip + irr_center_pivot + irr_flood
irr_center_pivot = 0
irr_flood       = 0
efficiency_old  = weighted avg with old method mix
efficiency_new  = weighted avg with new method mix
pumping_af_yr   → pumping_af_yr × (efficiency_new / efficiency_old)
```

With METHOD_EFFICIENCY values, a county at 85/05/05/05 (CP/flood/drip/dryland) drops pumping by ~32% after conversion.

### 4. `corn_reduction_25`

Corn acreage drops 25%; reallocated 50% sorghum, 30% wheat, 20% dryland.

**Math:**
```
moved         = acres_corn × 0.25
acres_corn    → acres_corn × 0.75
acres_sorghum += moved × 0.50
acres_wheat   += moved × 0.30
irrigated_acres_total -= moved × 0.20  (the dryland slice leaves irrigation)
ag_value_usd  = recomputed from new crop mix
pumping_af_yr = recomputed from new crop mix × efficiency
```

### 5. `no_ag_below_9m`

Counties with `saturated_thickness_m < 9.0` stop all irrigation.

**Math:** For `fips` where `thickness < 9`:
```
all acres_{crop} → 0
irrigated_acres_total → 0
pumping_af_yr   → 0
ag_value_usd    → 0
irr_dryland     → 1.0
other irr shares → 0.0
```

Counties above threshold are untouched. Intent: show the rural economic cost of hard-cutoff policy.

### 6. `custom`

Composes the other scenarios at fractional intensity. Params: `CustomScenarioParams`:
- `pumping_reduction_pct` ∈ [0, 1] — a flat pumping cut applied last.
- `corn_to_sorghum_shift_pct` ∈ [0, 1] — fraction of corn acres shifted, all to sorghum.
- `drip_adoption_pct` ∈ [0, 1] — fraction of center-pivot + flood acres moved to drip.

Order of application: corn shift → drip adoption → pumping cut. With all three at 0, equals `status_quo`. Scaling `pumping_reduction_pct` linearly scales CO2 delta (asserted by `test_custom_pumping_reduction_scales_linearly`).

---

## Running

```python
from aquiferwatch.analytics.scenarios import run_scenario
from aquiferwatch.schemas import ScenarioID, CustomScenarioParams

result = run_scenario(ScenarioID.KS_LEMA_AQUIFER_WIDE, baseline_df)
print(result.aquifer_lifespan_extension_years)
print(result.embedded_co2_delta_mt)

custom = run_scenario(
    ScenarioID.CUSTOM, baseline_df,
    params=CustomScenarioParams(pumping_reduction_pct=0.30, drip_adoption_pct=0.50),
)
```

## Audit checklist

Every scenario's math must be:
1. Documented in this file with inline formulas.
2. Covered by a known-answer test in `tests/test_scenarios.py`.
3. Cited to a published source for any non-identity constant.
4. Deterministic (same input → same output; `test_same_input_same_output` is the bulwark).

When a scenario's math changes (e.g., updated Basso coefficient), update this file, the constant in code, and the known-answer test — in the same PR.
