# AquiferWatch: Ogallala Aquifer Accountability Platform

**Hackathon spec. 7-day build. Team of 2 (Raj + teammate).**

Codename: **AquiferWatch** (placeholder)
Tagline: *Who is draining the High Plains, what it's growing, and how long until it's gone.*

---

## 1. What this is

An interactive, county-level accountability map of the Ogallala/High Plains Aquifer spanning eight states. Three analytical layers:

1. **Depletion Map**: Current saturated thickness, historical decline 1950-present, projected years-until-uneconomic at current extraction rates. Counties color-coded by remaining aquifer lifespan under status quo.

2. **Extraction Attribution**: What crops are drawing this water, at what intensity, and at what economic return per acre-foot. Which counties are burning the most water per dollar of ag value.

3. **Counterfactual Scenario Engine**: Policymaker-selectable scenarios. "What if the Kansas LEMA model scaled aquifer-wide?" "What if corn acreage dropped 25% and shifted to sorghum/wheat?" "What if every county adopted drip irrigation?" Output: aquifer lifespan extension, ag profit delta, embedded CO2 delta, rural economic impact.

Built for journalists, policy staff, and state water-board analysts. Not for individual farmers. This means rigorous methodology, full downloadability, reproducible scenarios, and the kind of visual polish that earns a link from Canary Media or the New York Times climate desk.

---

## 2. The thesis the tool is arguing

The Ogallala supports $35B in annual agricultural production across eight states. It is being pumped at a rate that is not sustainable and cannot be replaced on human timescales. Current trajectories put 24% of currently-irrigated land over the aquifer below economic viability by 2100, with some Kansas and Texas counties already past the threshold. The tool does not preach. It just makes the math unavoidable.

Specific claims the tool lets the user verify themselves:
- Which counties will run dry first
- Which crops are concentrating extraction
- Where adaptation policies have worked (Sheridan-6 LEMA in Kansas) and where they haven't
- What the counterfactual cost of inaction has been since 2000

---

## 3. Scope boundaries

### In scope
- 8 states over the High Plains Aquifer: Nebraska, Kansas, Colorado, Texas, Oklahoma, New Mexico, South Dakota, Wyoming
- County-level resolution (~180 counties over the aquifer)
- 1950-present historical aquifer data + 2025-2100 projections
- Top 6 irrigated crops: corn, soybeans, sorghum, wheat, cotton, alfalfa
- Economic outputs: $ per acre-foot, cumulative dollar-weighted extraction
- Policy overlay: LEMAs in Kansas, groundwater conservation districts in Texas, state-level rules

### Out of scope
- Sub-county resolution (possible v2)
- Confined aquifer interactions (stick to the unconfined High Plains)
- Municipal and industrial water use (only 6% of extractions; flag but don't model)
- Full climate-projection integration (use USDA climate-adjusted yield factors instead of running your own climate models)
- Livestock water (secondary relative to irrigation)

### Known limitations documented in the methodology page
- Saturated thickness data is interpolated from monitoring well networks with varying density (Texas is well-monitored, Wyoming and SD less so)
- "Years-until-uneconomic" depends on pumping cost assumptions that vary by energy prices
- County-level crop water use is modeled, not metered, for most counties
- Counterfactuals assume static climate (a real limitation, but the alternative is a whole additional modeling layer)

---

## 4. Data sources

### Primary

| Source | What it gives | Status |
|---|---|---|
| USGS Ground Water Atlas + High Plains Aquifer monitoring | Saturated thickness, water levels over time, well network | Public, reliable |
| USDA NASS Quick Stats | County-level crop acreage, yields, irrigation status | You already have this plumbed |
| USDA Irrigation and Water Management Survey (2023, released Nov 2024) | Water applied per acre by crop and irrigation method, by state/region | Recent release, use it |
| USDA Census of Agriculture 2022 | Baseline farm counts, irrigated acreage, operator demographics | Public |
| Kansas Geological Survey WIMAS | Kansas-specific extraction data by well | Best-in-class; Kansas is our pilot state for rigor |
| Texas Water Development Board | Texas well registration, Groundwater Conservation District data | Public |
| Nebraska DNR | Registered wells, use reports | Public |
| NRCS EQIP and Ogallala Aquifer Initiative | Policy intervention locations and outcomes | Public |

### Secondary

| Source | Purpose |
|---|---|
| Deines et al. 2019 aquifer depletion projections | Use as methodology reference and validation |
| USDA ERS county-level crop budgets | $ per acre for each crop-irrigation combination |
| EIA state electricity prices | Pumping cost modeling |
| USGS Water Use Data (quinquennial) | Cross-validation of extraction estimates |
| NOAA climate normals | Precipitation baselines for dryland-vs-irrigated comparisons |

### The politically-sensitive limitation

Texas operates under the "rule of capture." Most Texas counties do not require metered extraction reporting. This means Texas numbers are modeled, not observed. Do not hide this in the methodology; put it on the map itself as a data-quality indicator. Kansas and Nebraska are better instrumented. This heterogeneity is part of the story, not a bug.

---

## 5. Technical architecture

### Stack

**Data pipeline**
- Python 3.11, Poetry
- DuckDB as analytical warehouse (you already use this)
- Polars for transforms
- GDAL/GeoPandas for geospatial ops
- Great Expectations for data quality gates

**Modeling**
- LightGBM for imputation where county-level extraction is unreported (Texas especially)
- Simple linear depletion models per county for short-horizon projection
- Deines-style methodology for long-horizon projection (saturated thickness + recharge + pumping rate → years-until-9m-threshold)
- scikit-learn baselines

**Backend**
- FastAPI
- PostGIS for county polygon joins and aquifer boundary ops
- Parquet files for the public data download feature

**Frontend**
- Next.js 14
- deck.gl for the map (you pushed back on my frontend-caution earlier; deck.gl stays)
- Mapbox GL JS basemap, dark muted theme
- Framer Motion for scenario transitions
- Tailwind + Cobbles & Currents v3 design tokens
- D3 for the supporting charts (depletion curves, crop mix donuts, sankey for water flows)

**Deployment**
- Backend on Railway or Fly.io
- Frontend on Vercel
- Static parquet dumps on Cloudflare R2 for the download feature

### Repository layout

```
aquiferwatch/
├── apps/
│   ├── web/              # Next.js + deck.gl
│   └── api/              # FastAPI
├── packages/
│   ├── pipeline/         # Ingest scrapers, USGS, NASS, state GW boards
│   ├── models/           # Pydantic schemas shared
│   └── analytics/        # Depletion projections, scenario engine
├── data/
│   ├── raw/              # (gitignored) original downloads
│   ├── interim/          # cleaned county tables
│   └── processed/        # publication-ready parquet
├── notebooks/            # exploration + validation
├── scripts/
└── docs/
    ├── methodology.md
    ├── data_sources.md
    ├── limitations.md
    └── scenarios.md      # how each counterfactual is computed
```

---

## 6. The scenario engine (this is the differentiator)

Every scenario is a deterministic function of a clearly-documented input vector and transparent math. No black boxes. Policymakers and journalists can point to the scenario page and verify the calculation.

### Built-in scenarios (v1)

**1. Status Quo**
Current extraction rates continue. Current crop mix continues. Shows years-until-uneconomic per county.

**2. Kansas LEMA Aquifer-Wide**
What if every county adopted the Sheridan-6 LEMA rules (~25-30% pumping reduction, cropping shifts toward sorghum/wheat)? Sourced from the 2025 Basso et al. environmental footprint paper.

**3. Drip Irrigation Transition**
What if every center-pivot and flood-irrigated acre shifted to drip/microirrigation over 10 years? Apply 30-40% water savings factor to relevant crops.

**4. 25% Corn Reduction**
Corn acreage drops 25%, reallocated to sorghum (50%), wheat (30%), dryland (20%). Apply water use deltas.

**5. No Ag Over 9m Threshold**
What if counties below 9m saturated thickness stopped pumping entirely? Show the rural economic cost.

**6. Custom Scenario**
User-adjustable sliders for: % pumping reduction, % corn-to-sorghum shift, % irrigation method transition. Outputs update live.

### Outputs of every scenario

- Aquifer lifespan extension (years)
- Cumulative ag production delta ($B over 25 years)
- Rural employment delta (using IMPLAN-style multipliers from USDA ERS)
- Embedded CO2 delta (less pumping = less electricity = less emissions; use EIA grid intensity by state)
- Per-county winners and losers

---

## 7. Team split

Your teammate is Python/ML/DE strong, similar experience level, Luddy ADS. You're full-stack + design + narrative.

### Teammate owns
- USGS and state groundwater data ingestion (this is the ugliest data; well worth them owning it)
- Saturated thickness interpolation and depletion projection model
- County-level extraction imputation (LightGBM for Texas where unreported)
- Crop-water coefficient database construction from USDA Irrigation Survey
- Scenario engine math (pure Python, unit-tested)

### You own
- Frontend entirely: deck.gl, Next.js, Framer Motion, design system
- FastAPI design, PostGIS setup, API contract
- The methodology documentation (journalist-facing writeup)
- The scenario UI
- The three featured stories (see section 9)
- Deployment and CI/CD

### Paired
- Pydantic schemas (day 0)
- Economic overlay (ERS crop budgets, IMPLAN multipliers)
- Validation and QA

### Interfaces to lock Day 0
- `County`, `AquiferSection`, `CropWaterFootprint`, `Scenario`, `ScenarioResult` Pydantic models
- Parquet conventions for the warehouse
- API routes: `/counties`, `/scenarios`, `/scenarios/:id/results`, `/counties/:fips/history`

---

## 8. Seven-day milestone plan

Assumes ~8-10 focused hours per day per person.

### Day 0 (prep night)
- Repo scaffolded (pnpm workspaces, Poetry envs)
- Mapbox token, PostGIS in Docker
- Pydantic schemas drafted
- Next.js app with design system colors/fonts loaded
- FastAPI skeleton responding at localhost:8000
- 30-min kickoff call: walk through this spec, lock interfaces

### Day 1: Foundation and geography
**Teammate:** USGS High Plains Aquifer shapefile ingested, county-aquifer intersection computed. USGS water level monitoring well data scraped for 1990-present. Landed in DuckDB.
**You:** Base map renders 8 states, aquifer boundary overlayed, counties rendering as choropleth skeleton. FastAPI returns mock scenario results.
Checkpoint: You can see the 8-state aquifer on screen with Cobbles & Currents styling.

### Day 2: Extraction and crops
**Teammate:** NASS Quick Stats for top 6 crops by county, 2000-2023. USDA Irrigation Survey water-application rates joined. State groundwater board data ingested where metered (KS, NE).
**You:** Real depletion layer rendering per county (color = years-until-uneconomic). Click-to-detail panel showing county's crop mix and extraction estimate.
Checkpoint: Map shows real data. A user clicking Sherman County Kansas sees real numbers.

### Day 3: Projection and imputation
**Teammate:** Depletion projection model per county. Texas extraction imputation model (LightGBM from acreage + crop mix + monitoring-well-based proxy). Validation against Deines 2019 for Kansas counties.
**You:** Time-scrubber UI 1950-2050. Scrubbing animates saturated thickness change historically and projected. Start on scenario selector UI scaffolding.
Checkpoint: The "watch the aquifer empty over 100 years" visual works. This is the demo anchor.

### Day 4: Scenario engine
**Teammate:** Scenarios 1-5 implemented in `packages/analytics/scenarios.py`. Unit-tested. Exposed via `/scenarios/:id/run` endpoint.
**You:** Scenario UI fully working. Select a scenario, watch the map recolor, watch the stats panel animate to new values. Build scenario #6 (custom sliders) last if time.
Checkpoint: Demo-worthy scenario flow. Select "Kansas LEMA aquifer-wide" and watch 23 years of aquifer life come back on the map.

### Day 5: Economic and emissions overlays
**Teammate:** ERS crop budgets joined, $ per acre-foot computed per county. Rural employment multipliers. EIA grid intensity for embedded CO2.
**You:** Supporting chart panels: depletion curves per county, crop-mix sankey, economic impact bars. Methodology page written with transparent formulas.
Checkpoint: Every number on screen traces to a specific line in methodology.md.

### Day 6: Featured stories and polish
**You:** Three featured-story pages that tell a specific journalism-grade story using the tool:
1. **Sheridan-6 Kansas LEMA**: The one place that worked. Show before/after.
2. **Dallam County, TX**: The cautionary tale. Top 1% extraction, unmonitored, 12 years of economic life left.
3. **The Nebraska exception**: Best-instrumented state, strongest conservation districts, slowest depletion. Why.
**Teammate:** Data quality pass. Known-limitations documentation. Help with story copy.
Checkpoint: Site looks like something Canary Media would link.

### Day 7: Demo prep + publication
- Deploy, test cold load times
- Record backup walkthrough video
- 5-min demo script locked
- One-page summary for judges
- LinkedIn milestone post drafted (use the milestone-post skill)
- Optional: Draft outreach note to 2-3 journalists who cover this beat (Carey Gillam, Alex Prud'homme, Tim McDonnell)

---

## 9. What a good demo looks like

Open cold. Map of the 8-state High Plains region. The aquifer outline glows against muted state boundaries. Counties colored in a gradient from deep teal (centuries of life left) to amber (50 years) to burnt umber (already uneconomic). 180 counties, each one a real story.

Scrub the time slider from 1950 to 2025. Watch the map darken county by county. Kansas counties along the 100th meridian turn amber by the 1990s, burnt umber by 2010s. Dallam and Sherman counties in Texas go dark. Nebraska's Sandhills stay relatively stable.

Click Finney County, Kansas. A detail panel slides in. 247,000 irrigated acres, 68% corn by value, estimated 1.4 million acre-feet extracted annually, grid carbon intensity of pumping electricity, $ per acre-foot, years-until-uneconomic: 31.

Scrubber projects to 2050. 2100. The dark spread continues. The numbers update.

Hit "Scenario: Kansas LEMA Aquifer-Wide." The scenario panel opens. Watch an animated recoloring. Finney County goes from 31 years to 54 years remaining. Ag production drops 12%. Rural employment -4%. Embedded CO2 -18%. Aquifer lifespan extended by 23 years region-wide.

Try "25% corn to sorghum/wheat." Different tradeoff curve.

Scroll down. Three featured stories. Each with its own deep-dive page. Each grounded in the same data, each with a clear human-readable narrative.

Methodology link. Every formula. Every data source. Download buttons for the processed parquet.

That's the demo.

---

## 10. Post-hackathon path

**Immediate (week 2):**
- Clean up methodology doc into standalone publication
- Submit to Environmental Research: Infrastructure and Sustainability, or Earth's Future (where the 2013 Basso Ogallala paper ran)
- Co-authorship conversation with anyone at IU O'Neill School who works on water policy

**Portfolio (week 2-4):**
- Cobbles & Currents project page with the swimlane architecture style
- LinkedIn milestone post with the "scroll-stop" visual
- Add to rvedire.com under Engineer/Launch stages

**QuickStats Phase 2 (month 2+):**
This is where Shape C (AdaptLag) comes in. AquiferWatch is the accountability-focused, retrospective-and-counterfactual view of one specific aquifer. AdaptLag would be the forward-looking, adaptation-gap view across the continental US.

The handoff works like this:
- QuickStats has the commodity-first architecture (corn, soy, sorghum, wheat, cotton, etc.) with yield forecasting and price forecasting
- AdaptLag adds a third module: "adaptation efficiency per county" — comparing observed crop mix evolution against climate-suitability-optimal crop mix evolution
- Reuses AquiferWatch's crop-water-footprint database
- Reuses QuickStats' yield and price models
- New contribution: the climate-suitability model (Rising & Devineni methodology, or simpler regression baseline)

This three-project stack (QuickStats + AquiferWatch + AdaptLag) becomes a coherent "agricultural resilience under climate stress" portfolio theme. Three different lenses: forecasting (QuickStats), resource accountability (AquiferWatch), adaptation gap (AdaptLag). Ties to OC job market (ag tech is less relevant there, but the data engineering and geospatial work translates), ties to Wanless/SII (adjacent methodology), ties to any agricultural economics group you might want to collaborate with.

**Publication angle:**
- AquiferWatch alone: Environmental Research Letters or Earth's Future
- The three-project combination: a methodology paper on "open-source tooling for agricultural resilience accounting"

---

## 11. Risks and pre-mortem

**Risk 1: USGS well data is uglier than expected.**
Mitigation: Kansas Geological Survey WIMAS is clean and well-documented. Start there on Day 1. If USGS-wide aggregation takes too long, scope down to Kansas-only for the hackathon demo and expand post-hackathon.

**Risk 2: County-level extraction imputation for Texas is fragile.**
Mitigation: Be transparent about it. Show a data-quality indicator on the map. Texas is the cautionary tale anyway; that story is part of the project.

**Risk 3: Scenario engine gets too complex.**
Mitigation: Lock scenarios 1-4 by end of Day 4. Scenario 5 and 6 (custom sliders) are nice-to-haves. Cut ruthlessly.

**Risk 4: Deck.gl rendering issues with complex choropleth.**
Mitigation: You said you can handle it in one session. Front-load any unfamiliar deck.gl work on Day 0 prep. GeoJsonLayer is the one you'll lean on.

**Risk 5: Methodology gets challenged by a judge who actually knows this space.**
Mitigation: Welcome it. The methodology doc should be air-tight. Every formula traceable to a published source. If challenged, the response is "great question, here's the exact formula and the Deines 2019 reference." This is a feature, not a risk.

**Risk 6: You and your teammate fall out of sync on the scenario engine contract.**
Mitigation: Day 0 lock on the Pydantic schema. Write out three example scenarios on paper with concrete numbers before any code gets written. Revisit at end of Day 2 if anything feels off.

---

## 12. Naming

AquiferWatch is placeholder. Alternatives to consider:
- **Drawdown** (strong, ambiguous between water and carbon, available as a metaphor but taken as a brand by Project Drawdown — skip)
- **Saturated** (clever water-science double-meaning, available)
- **HighPlains** (literal, forgettable)
- **ThickToZero** (refers to "saturated thickness to zero" — insider term, strong with policy audience)
- **The Ogallala Project** (journalism-style, evocative)
- **Runoff** (too environmental-activist coded)

Pick Day 0 so the repo name doesn't haunt you.

---

## 13. Day 0 checklist

- [ ] Repo created: `aquiferwatch` or chosen name
- [ ] pnpm workspaces structure
- [ ] Poetry envs for pipeline, api
- [ ] Docker compose with PostGIS
- [ ] DuckDB CLI installed
- [ ] Mapbox token provisioned
- [ ] Pydantic schemas drafted in `packages/models/`
- [ ] Next.js app bootstrapped with Tailwind, design tokens loaded
- [ ] FastAPI hello-world responding
- [ ] Teammate has repo access and local env running
- [ ] 30-minute kickoff: spec walkthrough, interface lock, Day 1 commitments
- [ ] First USGS monitoring well data pulled successfully as a smoke test
