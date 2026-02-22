# Fantasy Baseball Projections Pipeline

This project pulls Fangraphs hitter projections (`BATX`, `Steamer`, `OOPSY`, `ATC`), builds your weighted per-PA aggregate, applies starter-pool z-scoring, converts to dollars, merges NFBC ADP, and syncs outputs to Google Sheets.

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

## Web app (local)

A Shiny app is included at:

- `app.R`

Launch it locally:

```bash
Rscript -e "shiny::runApp('.', host = '127.0.0.1', port = 8080)"
```

Open:

- `http://127.0.0.1:8080`

## Publish to a shareable URL (shinyapps.io)

1. Create a [shinyapps.io](https://www.shinyapps.io/) account (Free tier works).
2. In R, install deploy package once:

```bash
Rscript -e "install.packages('rsconnect', repos='https://cloud.r-project.org')"
```

3. In shinyapps.io dashboard:
   - Go to **Account -> Tokens**
   - Create/copy `name` (account), `token`, and `secret`
4. Set credentials in terminal:

```bash
export SHINYAPPS_ACCOUNT="your_account_name"
export SHINYAPPS_TOKEN="your_token"
export SHINYAPPS_SECRET="your_secret"
```

5. Deploy:

```bash
cd "/Users/ckaufman/Documents/New project"
Rscript scripts/deploy_shinyapps.R . fantasy-baseball-projection-updater
```

After publish, your URL will look like:

- `https://<account>.shinyapps.io/fantasy-baseball-projection-updater`

Re-publish after code changes with the same deploy command.

Current app features:

- editable projection system inclusion + weights
- editable stat category weights
- league format selector (`10`, `12`, `15` teams)
- ADP draft type + date-window controls
- preset loader (`Power + PT`, `Balanced`, `Speed Lean`)
- interactive sortable output table

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

For Shiny deployment:

- `shiny`
- `DT`
- `bslib`
- `rsconnect`

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
