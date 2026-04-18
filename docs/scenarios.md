# Scenarios

Implementation lives in `aquiferwatch/analytics/scenarios.py`. Each scenario is a pure function of `(baseline, params) → ScenarioResult`.

| ID | Display | Parameters | Source |
|---|---|---|---|
| `status_quo` | Status Quo | none | Baseline definition |
| `ks_lema_aquifer_wide` | Kansas LEMA, aquifer-wide | 25–30% pumping reduction, crop shift to sorghum/wheat | Basso et al. 2025 |
| `drip_transition` | Drip irrigation transition | 30–40% water savings on center-pivot + flood acres over 10y | USDA NRCS irrigation water management data |
| `corn_reduction_25` | 25% corn reduction | Corn → 50% sorghum / 30% wheat / 20% dryland | Reallocation per USDA ERS commodity reports |
| `no_ag_below_9m` | No ag below 9m | Counties below 9m saturated thickness stop pumping | Threshold per spec §6 |
| `custom` | Custom | `pumping_reduction_pct`, `corn_to_sorghum_shift_pct`, `drip_adoption_pct` | User-driven |

## Scenario math — template

Each scenario's docstring in `scenarios.py` points back to this file with a section like `## scenario: ks_lema_aquifer_wide` containing:

1. Assumed deltas (numeric, cited)
2. Which baseline columns are affected
3. How per-county output deltas roll up to aquifer-level summaries

These sections fill in as scenarios land (Day 4).
