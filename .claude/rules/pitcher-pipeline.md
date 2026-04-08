---
paths:
  - "scripts/build_pitcher_integration_table.R"
  - "scripts/calc_sp_skillz.R"
  - "scripts/derive_sp_skillz_weights.R"
  - "R/sp_skillz.R"
---

# Pitcher Pipeline Context

SP Skillz evaluation system + integrated pitcher table (projections, Eno ranks, ADP).

## Scripts & Modules
- `R/sp_skillz.R` — SP Skillz core: metric names, stabilization points, reliability methods, weight derivation logic
- `scripts/calc_sp_skillz.R` — standalone calculator: fetches Fangraphs leaderboard, applies weighted metrics, IP paradigm logic (low/high IP buckets), reliability weighting
- `scripts/derive_sp_skillz_weights.R` — derives metric weights from 2025 historical data via Fangraphs leaderboard correlation analysis
- `scripts/build_pitcher_integration_table.R` — merges pitcher projections + SP Skillz + Eno rankings + ADP into integrated table for Sheets export

## Key inputs (data/raw/)
- `2026_eno_feb3_ranks.csv` — Eno Sarris pitcher ranks
- `2026_ck_sp_ranks.csv` — custom CK pitcher ranks
- `2026_thebat_pitchers_raw.json`, `2026_steamer_pitchers_raw.json` — pitcher projections

## Key outputs (data/processed/)
- `2026_pitchers_integrated_table.csv` — main output (synced to Sheets)
- `2025_sp_skillz_scores.csv` — SP Skillz calculations
- `sp_skillz_composite_weights.csv` — derived metric weights

## Notes
- IP paradigm: pitchers split into low/high IP buckets with separate weighting logic
- SP Skillz uses stabilization-weighted reliability; metrics have different sample size thresholds
- Eno ranks are manually downloaded and stored in data/raw/ — not auto-fetched
- `build_pitcher_integration_table.R` accepts `--refresh-projections` and `--no-sheet-export` flags
