---
paths:
  - "R/fangraphs_projections.R"
  - "R/pipeline_config.R"
  - "scripts/build_ottoneu_points_csv.R"
---

# Hitter Pipeline Context

Core hitter projection, ranking, z-score, and dollar value logic. Also covers Ottoneu points conversion.

## Scripts & Modules
- `R/fangraphs_projections.R` — CORE: system codes, default weights, projection fetch/normalization, ADP merging, weighting logic, z-score calculation; contains STANDARD_COLUMN_MAP
- `R/pipeline_config.R` — YAML config loader with deep merge logic, defaults, path handling, validation
- `scripts/build_ottoneu_points_csv.R` — converts hitter projections to Ottoneu FG points scoring and exports CSV

## Key config (config/pipeline.yml)
- Projection systems & weights (default 3:2:3:1 BATX:Steamer:OOPSY:ATC)
- Category weights (PA, HR, SB, R, RBI, H)
- League setup, ADP filters, Google Sheets URLs

## Variable naming conventions
- `weighted_` prefix for weighted aggregates (e.g. `weighted_hr_per_pa`)
- `z_` prefix for z-scores; `_starter` suffix for starter-pool variants
- `_proj` suffix for projected counting stats (e.g. `hr_proj`)
- `dollars_*` for auction values
- `z_total_*` for valuation totals

## Name/team normalization
- `normalize_join_name()` → `name_key`: lowercase + punctuation stripped + ASCII via iconv
- `normalize_team_abbrev()` → `team_key`
- Use `Names` column (not `NameASCII`) for player names in datasets
- `player_match_overrides.csv` in data/manual/ for edge cases

## Main output
- `data/processed/2026_hitters_z_scored_aggregate_projection_output.csv` — main output, synced to Sheets
