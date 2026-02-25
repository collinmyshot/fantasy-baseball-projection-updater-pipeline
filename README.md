# Fantasy Baseball Projections Pipeline

This project pulls Fangraphs hitter projections (`BATX`, `Steamer`, `OOPSY`, `ATC`), builds your weighted per-PA aggregate, applies starter-pool z-scoring, converts to dollars, merges NFBC ADP, and syncs outputs to Google Sheets.

## Project boundary (explicit)

- This git/GitHub project is for **data pipeline + Google Sheets export only**.
- It **does not** run, deploy, or update the Shiny app.
- Shiny is handled as a separate local/deploy workflow outside this repo automation path.

## Configuration-first workflow

Most settings now live in:

- `config/pipeline.yml`

Key sections:

- projection systems + system weights
- custom category weights
- PA floor
- league/starter slot assumptions
- auction settings
- NFBC ADP filters (`draft_type`, `num_teams`, lookback days)
- Google Sheet URLs/tab names
- quality gates

## Core scripts

- `scripts/fetch_nfbc_adp.R`: downloads NFBC ADP TSV with configured filters
- `scripts/fetch_nfbc_adp.sh`: legacy wrapper that calls `fetch_nfbc_adp.R`
- `scripts/fetch_projections.R`: full projections + aggregation + z-score + dollar pipeline
- `scripts/run_pipeline.R`: one-command orchestrator (ADP -> projections -> Sheets sync + formatting)
- `scripts/calc_sp_skillz.R`: standalone SP Skillz calculation from your Fangraphs custom leaderboard
- `scripts/push_to_google_sheets.R`: writes final output CSV to `Projections_Bats`
- `scripts/sync_adp_tab.R`: rebuilds `ADP` tab from `Projections_Bats`
- `scripts/sync_run_data_tab.R`: rebuilds `Run Data` tab (run settings + weights + ADP window info)
- `scripts/sync_position_tabs.R`: rebuilds `C/1B/2B/3B/SS/OF` tabs, preserving `CK.Rank` + `DS.Rank`
- `scripts/setup_rbell_team_targets.R`: rebuilds `RBLL Team & Targets` template while preserving manual inputs
- `scripts/format_workbook_tabs.R`: bold/filter/freeze/center formatting for all tabs

## One-command run

```bash
Rscript scripts/run_pipeline.R
```

## Shiny app note

This repo is now pipeline-only (FG/NFC pulls, calculations, and Google Sheets sync).
Shiny app code/deploy assets are intentionally kept local and ignored by git.

Optional flags:

```bash
Rscript scripts/run_pipeline.R --skip-adp-download
Rscript scripts/run_pipeline.R --skip-sheets
Rscript scripts/run_pipeline.R --config config/pipeline.yml
```

## Manual run

```bash
Rscript scripts/fetch_nfbc_adp.R --config config/pipeline.yml
Rscript scripts/fetch_projections.R --config config/pipeline.yml
```

Standalone SP Skillz run:

```bash
Rscript scripts/calc_sp_skillz.R --config config/pipeline.yml
```

Legacy positional overrides still work for `fetch_projections.R`:

```bash
Rscript scripts/fetch_projections.R 2026 "batx,steamer,oopsy,atc" "batx=3,steamer=2,oopsy=3,atc=1" 200 260 0.70 1 15
```

## Output files

Main output:

- `data/processed/<season>_hitters_z_scored_aggregate_projection_output.csv`

Audit outputs (new):

- `data/processed/<season>_adp_match_summary.csv`
- `data/processed/<season>_adp_projection_unmatched.csv`
- `data/processed/<season>_adp_unmatched.csv`
- `data/processed/<season>_adp_matches.csv`

Other core intermediates:

- `<season>_hitters_weighted_per_pa.csv`
- `<season>_hitters_weighted_per_pa_zscores.csv`
- `<season>_hitters_weighted_per_pa_starter_pool.csv`
- `<season>_hitters_weighted_per_pa_dollar_reference.csv`
- `<season>_refresh_metadata.csv`
- `<season>_adp_download_metadata.csv`

## Manual player matching overrides

For edge cases (diacritics/suffixes/duplicate names), add mappings to:

- `data/manual/player_match_overrides.csv`

Columns:

- `projection_name`
- `projection_team`
- `adp_name`
- `adp_team`
- `notes`

## Quality gates

`fetch_projections.R` now validates:

- minimum row count per projection system
- minimum weighted aggregate rows
- minimum z-score rows
- finite dollar outputs
- ADP match-rate threshold (warning or fail by config)

All thresholds are configurable in `config/pipeline.yml` under `quality`.

## R package requirements

At minimum:

- `jsonlite`
- `yaml`

For Google Sheets scripts:

- `googlesheets4`

Optional reproducibility bootstrap:

```bash
Rscript scripts/init_renv.R
```

## Scheduled refresh

Workflow: `.github/workflows/daily-refresh.yml`

- Scheduled at `3:00 AM Pacific` (DST-safe via dual UTC schedules + Pacific hour/minute gate)
- Runs the full pipeline, including Google Sheets sync:
  `Rscript scripts/run_pipeline.R --config config/pipeline.yml`
- Pulls source-rank context from `google_sheets.source_ranks_url` and preserves manual `CK/DS` rank columns in position tabs
- Installs `jsonlite`, `yaml`, and `googlesheets4`
- Commits refreshed files in `data/` only
- Does not deploy or update shinyapps.io

### Required GitHub secret

To update Google Sheets from GitHub Actions (no local PC required), add this repo secret:

- `GSHEETS_SERVICE_ACCOUNT_JSON`: full JSON key for a Google service account

Then share both spreadsheets with that service-account email as `Editor`:

- target workbook (`google_sheets.workbook_url`)
- source ranks workbook (`google_sheets.source_ranks_url`) if different from target

## Park factor model (BBE, fantasy-first)

Builds park factors from batted-ball events using:

- outcome: `wOBAcon - xwOBAcon` at BBE level
- fantasy components: `BACON residual` (`hit_on_contact - xBA`) and `HR-on-contact`
- hierarchical random effects: park era, park-half, batter-season, pitcher-season, fielding team-season, batting team-season
- first/second half splits (`1H = Mar-Jun`, `2H = Jul-Sep`)
- default modeling window starts at `2015` (first Statcast year)
- default exclusion of `2020`
- venue-based park-era IDs (captures team moves/temporary parks) plus manual dimension-change overrides in `data/manual/park_era_events.csv`

Run:

```bash
Rscript scripts/build_park_factors.R --bbe-input data/raw/statcast_bbe.csv
```

Fetch/store Statcast BBE data (persistent chunk store, skips existing chunks):

```bash
Rscript scripts/fetch_statcast_bbe.R
```

This writes:

- chunk store: `data/raw/statcast_bbe_store_chunks/`
- chunk manifest: `data/raw/statcast_bbe_store_chunks/chunk_manifest.csv`
- combined BBE table: `data/raw/statcast_bbe_store.csv`

To add new seasons without re-scraping history:

```bash
Rscript scripts/fetch_statcast_bbe.R --start-season 2026 --end-season 2026
```

Optional:

```bash
Rscript scripts/build_park_factors.R \
  --bbe-input data/raw/statcast_bbe.csv \
  --defense-input data/manual/team_defense.csv \
  --measurement-era auto \
  --output-dir data/processed/park_factors
```

Optional defense input template:

- `data/manual/team_defense_template.csv` with `season,team,oaa,drs,uzr`

Key outputs (`data/processed/park_factors/`):

- `park_factors_by_half.csv`
- `park_factors_overall.csv`
- `park_factors_bacon_by_half.csv`
- `park_factors_bacon_overall.csv`
- `park_factors_hr_by_half.csv`
- `park_factors_hr_overall.csv`
- `validation_summary.csv`
- `validation_detail.csv`
- `measurement_era_comparison.csv`
- `invariance_checks.csv`
- `team_park_era_audit.csv`
- `team_park_transitions.csv`
- `run_metadata.csv`
