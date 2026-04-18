# Known limitations

Each limitation below MUST have a corresponding surface in the UI (per spec §3).

### 1. Monitoring well density varies
- KS, NE: dense, reliable. Data-quality flag = `metered` / `modeled_high`.
- TX: moderate density, no metered extraction. Flag = `modeled_low`.
- WY, SD: sparse. Flag = `modeled_low`.
- **UI surface:** per-county ring color on the map.

### 2. Texas rule of capture
Texas does not require metered extraction reporting in most counties. Our Texas numbers are modeled via `analytics.extraction_imputation`, not observed. **UI surface:** legend tooltip on any Texas county.

### 3. Pumping cost sensitivity
"Years-until-uneconomic" depends on energy prices. We use EIA state electricity averages; ±30% energy price swings translate into roughly ±4 years for the median HPA county. **UI surface:** methodology page footnote.

### 4. County-level crop water use is modeled
Metered at the state level (IWMS), allocated to counties by irrigated-acreage share. **UI surface:** methodology page §5.

### 5. Static climate assumption
Counterfactuals hold climate constant. This is a real limitation; the alternative is a whole additional modeling layer. **UI surface:** scenario panel footnote.

### 6. Aquifer-section heterogeneity
The "High Plains Aquifer" is not monolithic (Ogallala, Brule, Arikaree). We treat it as a single unit for policy-level messaging; sub-aquifer fidelity is a v2 goal.
