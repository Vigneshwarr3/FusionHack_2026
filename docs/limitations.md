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

### 4b. Inferred pumping under-counts by ~2x vs. USGS reported
Our baseline `pumping_af_yr` is computed as Σ(per-crop acres × IWMS water rate)
for 6 major crops. USGS's 2015 Estimated Use of Water dataset reports ~2x higher
HPA-wide irrigation groundwater withdrawals (18.1M AF vs. our inferred 9.5M AF;
correlation 0.70 across 558 overlap counties). Likely causes: (a) conservative
IWMS rates; (b) our 6-crop filter misses minor irrigated crops (vegetables,
orchards, nursery) that USGS captures; (c) Census acreage undercount vs. state
agency reports. **Consequence for scenarios:** percentage deltas (lifespan
extension, employment %) are unaffected by this bias since it applies to both
baseline and modified. Absolute CO₂-delta numbers (in Mt) are roughly 2x
understated. **UI surface:** side-by-side "our model vs. USGS reported"
tooltip on each county's detail panel — the bias is the story, not a bug.

### 5. Static climate assumption
Counterfactuals hold climate constant. This is a real limitation; the alternative is a whole additional modeling layer. **UI surface:** scenario panel footnote.

### 6. Aquifer-section heterogeneity
The "High Plains Aquifer" is not monolithic (Ogallala, Brule, Arikaree). We treat it as a single unit for policy-level messaging; sub-aquifer fidelity is a v2 goal.
