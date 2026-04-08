# Monthly In-Season Park Factor Refresh

## Overview

Automate monthly park factor updates during the MLB season (March–October). Each month, new Statcast BBE data is fetched, appended to the historical store, the mixed-effects model is re-fit, and updated park factors are exported to Google Sheets.

**Timing**: Runs on the **2nd of the following month** (e.g., April 2 captures all March data). This avoids Statcast's ~1 day data lag that could cause the last day of the month to be missed. The March run is special — it includes pre-season park change detection.

---

## New Files (6)

### 1. `scripts/run_park_factor_monthly.R` — Monthly orchestrator

Modeled after `scripts/run_pipeline.R` (reuses `run_rscript()` retry pattern, config loading via `R/pipeline_config.R`).

**CLI args**: `--config`, `--month` (YYYY-MM, defaults to previous month), `--skip-sheets`, `--skip-park-review`, `--force-validation`

**Step sequence**:
1. Parse args, load config, determine target month and whether this is a March (pre-season) run
2. **Park change review** (March only): call `scripts/detect_park_changes.R`
3. **BBE fetch**: call `scripts/fetch_statcast_bbe.R` with `--start-date` / `--end-date` scoped to target month, `--start-season` / `--end-season` for the current year
4. **Data quality checks**: call `scripts/validate_monthly_bbe.R` — abort on hard failures
5. **Model re-fit**: call `scripts/build_park_factors.R` with `--skip-validation` (unless March or `--force-validation`)
6. **Clean/export**: call `scripts/build_park_factor_clean_2026.R` with dynamic `--season-target`
7. **Convergence tracking**: call `scripts/track_pf_convergence.R`
8. **Run metadata**: append to `data/processed/park_factors/monthly_run_log.csv`

Each step wrapped in `tryCatch`. Steps 3–5 are fatal on failure. Steps 6–7 are warn-and-continue.

### 2. `scripts/detect_park_changes.R` — March park change review

**Two detection modes**:

**Mode 1 — Venue move detection (fully automated)**:
- Query MLB StatsAPI for current season's venue assignments (same logic as `scripts/fetch_mlb_home_parks.R`)
- Compare against previous year's `mlb_home_parks_{year}_verified.csv`
- Flag any team whose `venue_id` changed
- Auto-suggest new rows for `park_era_events.csv`

**Mode 2 — Dimension change search (semi-automated)**:
- For each team, query MLB.com team news feeds / news search for keywords: "dimension", "wall", "fence", "outfield", "renovation"
- Score results by keyword density and recency
- Output findings with confidence levels (high/medium/low)

**Output**: `data/processed/park_factors/park_change_review_{season}.csv` with columns: `team`, `change_type`, `confidence`, `evidence_url`, `summary`, `action_required`. Does NOT auto-modify `park_era_events.csv` — produces recommendations for manual review.

### 3. `scripts/validate_monthly_bbe.R` — Data quality checks

**Checks**:
1. Row count: minimum ~25K BBE per full month, flag if below
2. Date coverage: BBE dates should span the target month, flag gaps >2 days
3. Team coverage: all 30 teams appear as both home and away
4. Column completeness: `launch_speed`, `launch_angle`, `estimated_ba_using_speedangle` have <5% NA
5. Duplicate detection: `game_pk` + `batter` + `pitcher` + `game_date` uniqueness
6. Game count sanity: ~200–230 regular season games per month

**Output**: `data/processed/park_factors/bbe_quality_{YYYY-MM}.csv`. Exit code 0 = pass, 1 = warnings, 2 = hard fail.

### 4. `scripts/track_pf_convergence.R` — Month-over-month tracking

- Reads current month's `park_factors_savant_style_clean_{year}.csv`
- Appends to cumulative history: `data/processed/park_factors/pf_monthly_history.csv`
  - Schema: `run_date, month_label, team, park, overall_pf, bacon_pf, hr_pf, xbh_pf, total_bbe`
- Computes deltas from previous month per team
- Flags any team where `abs(delta_overall) > alert_threshold` (default: 3.0 index points)
- Writes: `data/processed/park_factors/pf_convergence_alerts_{YYYY-MM}.csv`
- Prints summary to stdout for GH Actions logs / `$GITHUB_STEP_SUMMARY`

### 5. `.github/workflows/monthly-park-factors.yml` — GitHub Actions workflow

```yaml
on:
  workflow_dispatch:
    inputs:
      month:
        description: 'Target month YYYY-MM (defaults to previous month)'
        required: false
      force_validation:
        description: 'Run full validation'
        type: boolean
        default: false
  schedule:
    # 2nd of Apr-Nov at 11 AM UTC (~4 AM PDT / 3 AM PST)
    - cron: '0 11 2 4-10 *'   # Apr-Oct (PDT)
    - cron: '0 12 2 11 *'     # Nov (PST, for October data)
```

**Steps**: Checkout → Setup R → Install deps (add `any::lme4`, `any::Matrix`, `any::httr`, `any::curl`) → Write GSheets service account key → Determine target month → Run orchestrator → Commit refreshed data → Upload convergence alerts as artifact → Write job summary

**Runtime**: ~20–40 min for non-March runs. ~30–45 min for March (with validation).

### 6. `.claude/rules/park-factors-monthly.md` — Claude rules file

Context for future sessions working on the monthly refresh: file locations, run cadence, how park_era_events.csv is maintained, alert thresholds.

---

## Existing Files to Modify (4)

### 1. `scripts/build_park_factors.R`

**Add `--skip-validation` boolean flag** to the `parse_cli_args` spec (line 4–18).

When `--skip-validation` is TRUE:
- Skip `rolling_validate_park_factors()` call (lines 193–199)
- Skip `compare_measurement_era_models()` call (lines 146–151) — use `--measurement-era off` instead
- Still write `validation_summary.csv` and `measurement_era_comparison.csv` with "skipped" markers so downstream consumers don't break on missing files

This is the single biggest performance lever — validation is the most expensive step.

### 2. `scripts/fetch_statcast_bbe.R`

**Add `--start-date` and `--end-date` flags** (date strings like `2026-04-01`).

In `build_chunk_table()` (line 298), when these are provided:
- Override season-based date range generation to only produce chunks within the specified date window
- This avoids iterating through hundreds of already-complete historical chunks
- The existing incremental skip logic (line 442) already handles re-runs, so this is a performance optimization for monthly scoping

### 3. `config/pipeline.yml`

**Add `monthly:` subsection** under `park_factors:`:

```yaml
park_factors:
  component_weights:
    bacon: 0.45
    hr: 0.35
    xbh: 0.20
  google_sheets:
    workbook_url: "..."
    overall_tab: "Overall PF"
    first_half_tab: "1H PF"
    second_half_tab: "2H PF"
    known_effects_tab: "Known Park Effects"
  monthly:
    bbe_store: "data/raw/statcast_bbe_store.csv"
    chunks_dir: "data/raw/statcast_bbe_store_chunks"
    output_dir: "data/processed/park_factors"
    convergence_alert_threshold: 3.0
    min_monthly_bbe: 25000
    exclude_seasons: [2020]
```

### 4. `R/pipeline_config.R`

Add defaults and normalization for the new `park_factors.monthly` config section in `default_pipeline_config()` and `normalize_pipeline_config()`.

---

## Data Flow

```
fetch_statcast_bbe.R (month-scoped)
  └─> data/raw/statcast_bbe_store.csv (appended)
        └─> build_park_factors.R (--skip-validation for non-March)
              └─> data/processed/park_factors/*.csv (all model outputs)
                    └─> build_park_factor_clean_2026.R
                          └─> Google Sheets (4 tabs)
                    └─> track_pf_convergence.R
                          └─> pf_monthly_history.csv (cumulative)
                          └─> pf_convergence_alerts_{YYYY-MM}.csv
```

---

## Key Design Decisions

1. **Full model re-fit every month** (no incremental update). The mixed-effects model pools random effects across all data — there's no way to incrementally update without refitting. For parks with 10 years of data, adding one month barely moves the estimate. For new parks (Sutter Health, KC walls-in), every month is highly informative.

2. **Skip validation for monthly runs**. Rolling validation proves the model specification is sound — it doesn't need to be re-proven monthly. Run it for the March pre-season baseline and on-demand via `--force-validation`.

3. **Convergence monitoring as a separate step** (not baked into the model script). This keeps `build_park_factors.R` focused on model fitting and makes the tracking independently testable.

4. **Park change detection produces recommendations, not auto-edits**. `park_era_events.csv` is a high-stakes manual file — auto-editing it risks silently corrupting the era assignments.

5. **Run on the 2nd of the following month** to avoid Statcast lag. April 2 captures all March data reliably.

---

## Testing Strategy

1. **Local dry run**: Run orchestrator with `--skip-sheets` and small `--max-rows` on `build_park_factors.R` for fast iteration
2. **BBE fetch test**: Test `--start-date`/`--end-date` on a known past month (e.g., July 2025)
3. **Park change detection test**: Run for 2026 — should detect ATH→Sutter Health, TBR→Steinbrenner, KCR walls-in
4. **Convergence tracking test**: Seed history file with baseline, run tracking, verify deltas
5. **GH Actions test**: `workflow_dispatch` with a specific month. Verify R deps install (especially `lme4` which needs `liblapack-dev`, `gfortran`)
6. **Output equivalence**: Verify `--skip-validation` produces identical park factor values to a full run (validation doesn't affect the final model fit)

---

## Implementation Order

1. `--skip-validation` flag on `build_park_factors.R` (smallest change, biggest impact)
2. `--start-date`/`--end-date` on `fetch_statcast_bbe.R`
3. `validate_monthly_bbe.R`
4. `track_pf_convergence.R`
5. `detect_park_changes.R`
6. `run_park_factor_monthly.R` (orchestrator, depends on all above)
7. `config/pipeline.yml` + `R/pipeline_config.R` updates
8. `.github/workflows/monthly-park-factors.yml`
9. `.claude/rules/park-factors-monthly.md`
10. Testing
