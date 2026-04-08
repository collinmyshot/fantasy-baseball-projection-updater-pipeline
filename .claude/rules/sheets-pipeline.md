---
paths:
  - "R/gsheets_auth.R"
  - "scripts/run_pipeline.R"
  - "scripts/push_to_google_sheets.R"
  - "scripts/sync_run_data_tab.R"
  - "scripts/sync_adp_tab.R"
  - "scripts/sync_position_tabs.R"
  - "scripts/setup_rbell_team_targets.R"
  - "scripts/format_workbook_tabs.R"
  - ".github/workflows/daily-refresh.yml"
---

# Sheets Pipeline Context

Daily automated pipeline: data fetch → processing → Google Sheets sync. Runs via GitHub Actions.

## Orchestration order (run_pipeline.R)
1. `fetch_nfbc_adp.R`
2. `fetch_projections.R`
3. `build_pitcher_integration_table.R --refresh-projections --no-sheet-export`
4. `sync_run_data_tab.R` → `push_to_google_sheets.R` → `sync_adp_tab.R` → `sync_position_tabs.R` → `setup_rbell_team_targets.R` → `format_workbook_tabs.R`

## Scripts
- `R/gsheets_auth.R` — auth helper: OAuth for local, service-account JSON for CI
- `scripts/run_pipeline.R` — main orchestrator with retry logic and fallback handling
- `scripts/push_to_google_sheets.R` — writes aggregated hitter projections to `Projections_Bats` tab
- `scripts/sync_run_data_tab.R` — populates `Run Data` tab with pipeline metadata, weights, ADP window
- `scripts/sync_adp_tab.R` — rebuilds `ADP` tab from `Projections_Bats` data
- `scripts/sync_position_tabs.R` — rebuilds C/1B/2B/3B/SS/OF tabs; preserves manual CK.Rank and DS.Rank columns
- `scripts/setup_rbell_team_targets.R` — rebuilds `RBLL Team & Targets`; preserves user manual inputs
- `scripts/format_workbook_tabs.R` — bold headers, auto-filter, freeze panes, center alignment
- `.github/workflows/daily-refresh.yml` — scheduled at 3 AM Pacific (DST-safe dual UTC crons + Pacific gate)

## Notes
- Google Sheets URLs live in `config/pipeline.yml` — do not hardcode them in scripts
- CI uses service-account JSON secret (GCP_SERVICE_ACCOUNT_JSON); local uses OAuth
- `sync_position_tabs.R` and `setup_rbell_team_targets.R` must preserve manual columns — never overwrite them
- No shinyapps.io deploy from this workflow — Shiny is separate and on-demand only
- Sheet tab names are fixed: Projections_Bats, ADP, Run Data, C, 1B, 2B, 3B, SS, OF, RBLL Team & Targets
