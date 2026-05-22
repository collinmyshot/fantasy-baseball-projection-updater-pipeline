# Fantasy Baseball Projections — Claude Code Context

## Project Root
Repo: https://github.com/collinmyshot/fantasy-baseball-projection-updater-pipeline

Local paths (machine-dependent — use whatever is correct for the current device):
- Mac:     `/Users/ckaufman/Documents/` (or wherever cloned)
- Windows: `C:\Users\Collin\OneDrive\Documents\` (or wherever cloned)

Active branch for Shiny work: `claude/frosty-moore`

## Who I Am
- Name: My Dude
- Background: PhD in neuroscience/biology, Field Application Scientist
- Coding: Confident in R, some Python
- Fantasy context: Ottoneu/NFBC formats, starter-only pools, emphasis on playing time

## Collaboration Rules

### ⚠️ RULE #1 — PARAMOUNT, NEVER BREAK:
**NEVER fabricate, guess, or estimate any data value, statistic, target, or factual claim.**
- If real data is not available, say so explicitly and ask for it or find a real source
- Never substitute a "reasonable estimate" for actual data — no exceptions
- This applies to hardcoded targets, projections, historical values, percentiles, etc.
- Require a real, citable source for every numeric fact put into the codebase
- If unsure whether a value is real or fabricated, stop and verify before proceeding

### Other Rules:
- Always ask before changing approach or refactoring — never do it unilaterally
- Work step-by-step; pause and check in at decision points
- Require multiple sources for factual/external claims
- No hallucinations — if unsure, say so
- Warm, friendly, playful tone — not cold or corporate
- R-first; Python only when clearly better/faster

---

## Two Workstreams

### 1. Pipeline (main branch)
Hitter/pitcher projection pipeline: FanGraphs projections, z-scores, dollar values, ADP merge, park factors model, GitHub Actions daily refresh.

**Run the full pipeline:**
```bash
Rscript scripts/run_pipeline.R --config config/pipeline.yml
```

**Pipeline order inside run_pipeline.R:**
1. fetch_nfbc_adp.R
2. fetch_projections.R
3. build_pitcher_integration_table.R --refresh-projections --no-sheet-export
4. If Sheets sync: sync_run_data_tab.R → push_to_google_sheets.R → sync_adp_tab.R → sync_position_tabs.R → setup_rbell_team_targets.R → format_workbook_tabs.R

### 2. Shiny App (claude/frosty-moore branch)
Active Shiny app lives in `fbb-tools/` subdirectory. Deployed to shinyapps.io.
- Account: `collinmyshot`
- App name: `fantasy-baseball-tools`
- Live URL: https://collinmyshot.shinyapps.io/fantasy-baseball-tools/

**Run locally:**
```bash
Rscript -e "shiny::runApp('fbb-tools/', host='127.0.0.1', port=8080)"
```

---

## Folder Structure

```
repo-root/
├── CLAUDE.md
├── README.md
├── config/
│   └── pipeline.yml               # Systems, weights, sheet URLs, ADP filters
├── R/                             # Pipeline-only shared utilities
│   ├── fangraphs_projections.R
│   ├── gsheets_auth.R
│   ├── park_factors.R
│   ├── pipeline_config.R
│   └── sp_skillz.R
├── scripts/                       # Executable pipeline scripts
│   └── run_pipeline.R             # MAIN ORCHESTRATOR
├── data/
│   ├── manual/                    # Hand-curated inputs
│   ├── raw/                       # Downloaded inputs
│   └── processed/                 # Pipeline outputs
│       └── park_factors/
├── fbb-tools/                     # ← ACTIVE SHINY APP (claude/frosty-moore branch)
│   ├── app.R                      # Shiny UI/server
│   ├── R/
│   │   ├── modules/               # 29 Shiny modules
│   │   ├── inseason/              # Step scripts (step1–step5); run interactively
│   │   ├── sp_skillz.R
│   │   ├── rp_skillz.R
│   │   ├── position_eligibility.R
│   │   └── utils_names.R
│   ├── data/                      # CSVs and park factors the app reads
│   └── www/                       # Logos, favicon, methodology HTML
├── rsconnect/                     # shinyapps.io deploy metadata
└── .github/workflows/             # GitHub Actions automation
```

**Note:** Any `fantasy-baseball-tools/` directory at the root is DEPRECATED. Ignore it. `fbb-tools/` is canonical.

---

## Inseason Step Scripts
`fbb-tools/R/inseason/step1–step5` are local analysis scripts run interactively (RStudio Source button or `Rscript`). They are NOT part of the deployed Shiny app.

Each step has a CONFIG block at the top with paths that need updating each week:
- `XLSX_PATH` / `FANTRAX_PATH` — point to your local Downloads folder
- `WEEK_START` / `WEEK_END` — current scoring week

**Cross-platform path prefixes:**
- Mac:     `/Users/ckaufman/Downloads/<filename>`
- Windows: `C:/Users/Collin/Downloads/<filename>`

The `base` variable (used to source shared R files) is resolved automatically via `rstudioapi` or `commandArgs` — no manual update needed.

---

## Packages
### Core: shiny, DT, bslib, rsconnect, dplyr, readr, ggplot2
### Also used: googlesheets4, jsonlite, yaml, lme4, rvest, markdown, renv, openxlsx, curl, httr

---

## Variable Naming Conventions
- General: snake_case throughout
- FG columns normalized via STANDARD_COLUMN_MAP → canonical names (name, team, pa, hr, rbi)
- Name matching: `name_key = normalize_join_name()` — lowercase + punctuation stripped + ASCII via iconv
- Team matching: `team_key = normalize_team_abbrev()`
- Weighted aggregates: `weighted_` prefix (weighted_hr_per_pa)
- Z-scores: `z_` prefix + `_starter` variants (z_hr, z_hr_starter)
- Projected counting stats: `_proj` suffix (hr_proj, rbi_proj)
- Valuation columns: z_total_*, dollars_*
- ADP audit columns: adp_match_method, adp_match_quality, name_team_flag
- Use Names (not NameASCII) for player name column in datasets

---

## External Data Sources
- FG projections: https://www.fangraphs.com/api/projections
- FG leaders: https://www.fangraphs.com/api/leaders/major-league/data
- NFBC ADP: https://nfc.shgn.com/adp.php
- Statcast BBE: https://baseballsavant.mlb.com/statcast_search/csv
- OAA: https://baseballsavant.mlb.com/leaderboard/outs_above_average
- MLB Stats API: https://statsapi.mlb.com/api/v1/
- Google Sheets: URLs in config/pipeline.yml

---

## Key Projects
1. Hitter projection/ranking pipeline — ZiPS/ATC/etc, z-scores, dollar values, ADP merge
2. Pitcher eval / probables pipeline — SP Skillz, Eno ranks, ADP, park factors, streaming
3. Shiny app — interactive explorer, deployed to shinyapps.io
4. Park factors model — custom fantasy-oriented park factors + methodology article
5. Ottoneu points calculator — projection-to-FG-points pipeline, CSV output

## Scheduled Automation
- GitHub Actions: `.github/workflows/daily-refresh.yml`
- Runs full pipeline + Sheets sync daily at 03:00 Pacific (DST-safe)
- Does NOT deploy to shinyapps.io
