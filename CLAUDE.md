# Fantasy Baseball Projections — Claude Code Context
## Project Root
`/Users/ckaufman/Documents/New project`
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
## Workflow Entry Points
### Full pipeline:
```bash
Rscript "/Users/ckaufman/Documents/New project/scripts/run_pipeline.R" \
  --config "/Users/ckaufman/Documents/New project/config/pipeline.yml"
```
### Pipeline order inside run_pipeline.R:
1. fetch_nfbc_adp.R
2. fetch_projections.R
3. build_pitcher_integration_table.R --refresh-projections --no-sheet-export
4. If Sheets sync: sync_run_data_tab.R → push_to_google_sheets.R → sync_adp_tab.R → sync_position_tabs.R → setup_rbell_team_targets.R → format_workbook_tabs.R
### Shiny app (local):
```bash
Rscript -e "shiny::runApp('/Users/ckaufman/Documents/New project', host='127.0.0.1', port=8080)"
```
### Scheduled automation:
- GitHub Actions: .github/workflows/daily-refresh.yml
- Runs full pipeline + Sheets sync daily at 03:00 Pacific (DST-safe)
- No shinyapps.io deploy from this workflow
---
## Folder Structure
```
New project/
├── app.R                          # Shiny UI/server
├── R/                             # Reusable modules
│   ├── fangraphs_projections.R    # Core hitter pipeline
│   ├── gsheets_auth.R             # Google Sheets auth
│   ├── park_factors.R             # Park factor utilities
│   ├── pipeline_config.R          # Config defaults + YAML
│   └── sp_skillz.R                # SP Skillz logic
├── scripts/                       # Executable pipeline scripts
│   └── run_pipeline.R             # MAIN ORCHESTRATOR
├── config/
│   └── pipeline.yml               # Systems, weights, sheet URLs, ADP filters
├── data/
│   ├── manual/                    # Hand-curated inputs
│   ├── raw/                       # Downloaded inputs
│   └── processed/                 # Pipeline outputs
│       └── park_factors/          # Park factor outputs
├── rsconnect/                     # shinyapps.io deploy metadata
└── .github/workflows/             # GitHub Actions automation
```
---
## Packages
### Core: shiny, DT, bslib, rsconnect, dplyr, readr, ggplot2
### Also used: googlesheets4, jsonlite, yaml, lme4, rvest, markdown, renv
---
## Variable Naming Conventions
- General: snake_case throughout
- FG columns normalized via STANDARD_COLUMN_MAP → canonical names (name, team, pa, hr, rbi)
- Name matching: name_key = normalize_join_name() — lowercase + punctuation stripped + ASCII via iconv
- Team matching: team_key = normalize_team_abbrev()
- Weighted aggregates: weighted_ prefix (weighted_hr_per_pa)
- Z-scores: z_ prefix + _starter variants (z_hr, z_hr_starter)
- Projected counting stats: _proj suffix (hr_proj, rbi_proj)
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
