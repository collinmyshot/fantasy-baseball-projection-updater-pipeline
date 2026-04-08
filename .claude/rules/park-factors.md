---
paths:
  - "R/park_factors.R"
  - "scripts/build_park_factors.R"
  - "scripts/build_park_factor_clean_2026.R"
  - "scripts/build_park_factor_display.R"
  - "scripts/build_park_factor_article.R"
  - "scripts/build_park_factor_article_docx.py"
  - "scripts/derive_park_factor_weights.R"
  - "scripts/log_park_factor_build_checkpoints.sh"
  - "scripts/log_xba_backfill_checkpoints.sh"
---

# Park Factors Context

Custom fantasy-oriented park factors model built on Statcast BBE data with hierarchical random effects.

## Scripts & Modules
- `R/park_factors.R` — utilities: text slugification, team abbreviation normalization
- `scripts/build_park_factors.R` — MAIN MODEL: hierarchical random-effects model (park era, park-half, batter-season, pitcher-season, fielding/batting team-season); rolling fold validation; outputs park factor tables + diagnostics
- `scripts/build_park_factor_clean_2026.R` — aggregates/cleans 2026 outputs; composite fantasy weights (BACON 45%, HR 35%, XBH 20%); exports to Google Sheets
- `scripts/build_park_factor_display.R` — Savant-style display table with fantasy weighting
- `scripts/derive_park_factor_weights.R` — bootstrap-based weight derivation with confidence intervals
- `scripts/build_park_factor_article.R` — generates markdown analysis article with figures
- `scripts/build_park_factor_article_docx.py` — converts markdown + figures to Word .docx (Python)
- `scripts/log_park_factor_build_checkpoints.sh` — live monitoring during build_park_factors.R run
- `scripts/log_xba_backfill_checkpoints.sh` — monitors xBA backfill chunk completion

## Key manual inputs (data/manual/)
- `mlb_home_parks_2026_verified.csv` — verified 2026 stadium data
- `park_era_events.csv` — manual park era IDs for relocations/renovations
- `team_defense_2015_2025.csv` — OAA composites by team-year

## Key outputs (data/processed/park_factors/)
- `park_factors_savant_style_clean_2026.csv` — main clean output
- `park_factors_bacon_overall.csv`, `park_factors_hr_overall.csv` — component factors
- `validation_summary.csv` / `validation_detail.csv` — rolling fold RMSE + correlation
- `BUILD_CHECKPOINT.md` — live build status during runs
- `article/park_factor_article_2026.md` — methodology article

## Notes
- This is an independent research/modeling project — runs separately from the daily pipeline
- build_park_factors.R is long-running; use the checkpoint monitoring scripts when running it
- Smoke test outputs live in `park_factors_smoke/` and `park_factors_smoke_resid_off/`
- Fantasy composite weights: BACON 45%, HR 35%, XBH 20%
