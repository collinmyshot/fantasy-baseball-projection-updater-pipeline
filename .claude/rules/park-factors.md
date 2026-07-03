---
paths:
  - "R/park_factors.R"
  - "scripts/build_park_factors.R"
  - "scripts/build_park_factor_clean_2026.R"
  - "scripts/build_park_factor_display.R"
  - "scripts/build_park_factor_article.R"
  - "scripts/build_park_factor_article_docx.py"
  - "scripts/derive_park_factor_weights.R"
  - "scripts/derive_park_factor_weights_v2.R"
  - "scripts/fetch_savant_drag.R"
  - "scripts/analyze_humidor_effect.R"
  - "scripts/log_park_factor_build_checkpoints.sh"
  - "scripts/log_xba_backfill_checkpoints.sh"
---

# Park Factors Context

**iPF (isolated Park Factor)**: hierarchical random-effects model on Statcast BBE data, built to isolate the park's own effect from players, defense, ball drag, and seasonal patterns. As of July 2026 the Overall factor is the main model's wOBAcon park effect taken DIRECTLY (no component blend; the old 45/35/20 BACON/HR/XBH composite is retired). BACON, HR, and Carry are displayed as component lenses.

**Publish path (only one):** copy the three **`_with_id`** clean CSVs (`park_factors_savant_style_clean_2026_with_id.csv` + `_1H_with_id` + `_2H_with_id`) to `/Users/ckaufman/Documents/fbb-tools-repo/data/park_factors/` and deploy the fbb-tools Shiny app via local rsconnect. As of 2026-07 the `_with_id` file is the SINGLE canonical source read by BOTH the PF leaderboard (`mod_park_factors.R`) AND the SP Streamonator/Outlook (`stream_load_pf`) — do NOT copy the non-`_with_id` display CSVs to fbb-tools (they are monorepo-only article/comparison inputs; copying them instead of the `_with_id` set was the old drift bug). `stuff_plus_by_park.csv` also lives in fbb-tools `data/park_factors/` (copy only when the Stuff+ pipeline regenerates it). The Google Sheets export is retired and its code removed — do not resurrect it.

## Scripts & Modules
- `R/park_factors.R` — model module: BBE standardization (incl. spray angle from hc_x/hc_y), contact-shape expected rates, residuals (wOBAcon, BACON, HR, XBH, 2B, 3B, Ottoneu points, air-ball Carry distance), lmer fits with drag/defense fixed effects (centered + missing indicator), rolling validation with composition-adjusted baseline comparison
- `scripts/build_park_factors.R` — MAIN MODEL: hierarchical random-effects model (park era, park-half, batter-season, pitcher-season, fielding/batting team-season); daily drag fixed effect (`--drag-input`, default data/processed/drag_daily.csv); `--max-date` caps the modeling window; rolling fold validation; outputs park factor tables + diagnostics + fixed-effects summary
- `scripts/fetch_savant_drag.R` — daily league ball drag coefficient from Savant drag dashboard (embedded serverVals JSON; cache-buster required); writes data/processed/drag_daily.csv (also the future Streamonator drag input)
- `scripts/build_park_factor_clean_2026.R` — aggregates/cleans 2026 outputs; re-standardizes indices over the 30 current parks; half tabs use the main model's park-half estimates directly
- `scripts/build_park_factor_display.R` — Savant-style display table: Overall = direct wOBAcon effect + BACON/HR/XBH/Carry component columns
- `scripts/derive_park_factor_weights.R` — v1 weight derivation (wOBAcon-reconstruction target; superseded — target guarantees BACON dominance)
- `scripts/derive_park_factor_weights_v2.R` — v2 weight derivation vs the Ottoneu-points park effect (free-coefficient regression, contribution shares, bootstrap CIs)
- `scripts/analyze_humidor_effect.R` — DiD research script for the 2022 universal humidor (use `--exclude-teams` for era-event confounds like BAL/DET/NYM)
- `scripts/build_park_factor_article.R` / `_docx.py` — analysis article generation
- `scripts/log_park_factor_build_checkpoints.sh` — live monitoring during build_park_factors.R run
- `scripts/log_xba_backfill_checkpoints.sh` — monitors xBA backfill chunk completion

## Key manual inputs (data/manual/)
- `mlb_home_parks_2026_verified.csv` — verified 2026 stadium data
- `park_era_events.csv` — manual park era IDs for relocations/renovations (each row needs primary sources)
- `team_defense_2015_2025.csv` — OAA/DRS/UZR composites by team-year (2026 not yet present — handled by defense_missing indicator)

## Key outputs (data/processed/park_factors/)
- `park_factors_savant_style_clean_2026_with_id.csv` (+ `_1H_with_id`/`_2H_with_id`) — the canonical outputs consumed by the fbb-tools app (both PF leaderboard and SP Streamonator/Outlook). The non-`_with_id` `park_factors_savant_style_clean_2026.csv` (+ `_1H`/`_2H`) are monorepo-only (article/comparison inputs) — NOT copied to fbb-tools
- `park_factors_{bacon,hr,xbh,double,triple,distance,points}_overall.csv` (+ `_by_half`) — component factors (distance = Carry in feet; points = Ottoneu FGpts per BBE)
- `park_factor_fixed_effects.csv` — fixed-effect estimates per model (drag/defense verification)
- `validation_summary.csv` / `validation_detail.csv` — rolling folds; `*_adj` columns are composition-adjusted comparisons
- `humidor_did_analysis.csv` — humidor DiD research output
- `_pre_drag_snapshot_20260702/` — pre-drag-correction outputs for before/after comparison

## Notes
- This is an independent research/modeling project — runs separately from the daily pipeline
- build_park_factors.R is long-running; use the checkpoint monitoring scripts when running it
- Daily drag covers 2016-04+ only; 2015 rows carry drag_missing = 1 (never fabricate missing drag values)
- BBE store chunk ids are per-run sequence numbers; stale cross-run duplicates for the same date window are ignored at combine time (quarantine dir: `_stale_2026_run/`)
- The points component (pts_resid) still runs in the build but is NOT displayed and is NOT referenced in the article anymore — slated for removal in the August refresh (see queue below)
- Article style (user requirement): no dashes as punctuation, no contractions, R² over Pearson r; article lives at fbb-tools-repo/www/methodology_park_factors.html with the 2026 iPF table + era appendix regenerated each refresh (scripts/build_park_era_appendix.R). Stability tables show mean absolute annual change only (R² column removed, sorted ascending).

## ⏭️ AUGUST 2026 REFRESH QUEUE (decided July, pilot-tested, ready to implement)
Three changes are pre-approved and validated for the next monthly rebuild. Do these together in one build:
1. **Month-bucket seasonal fixed effect** — replace the league-wide `half` fixed effect in fit_park_factor_model + fit_component_model with a 6-level month bucket (mar_apr, may, jun, jul, aug, sep_oct). KEEP the park×half random effects (`park_era_half_id`) and the 1H/2H tabs exactly as-is — only the league-level nuisance term changes. Pilot (scripts/pilot_month_effects.R): month structure beyond half is real (F=50.3, p≈2e-42; mixed-model month curve May +4.1 → Aug +11.5, ×1000 wOBAcon), but park effects are unaffected (half-vs-month park-era correlation 0.99994) — so this is a hygiene/cleanliness gain, will NOT move rankings > ~0.3 index pts. Sanity check the fitted month curve against Tango's ~10°F = +10% HR rule.
2. **Remove the points component** — delete the `points` spec from component_specs in build_park_factors.R + pts_resid/pts_on_contact/xpts_contact from R/park_factors.R. Confirmed dead: XBH re-derived against the SHIPPED target (direct wOBAcon effect) adds only +0.0012 R² (p=0.17) over BACON+HR, vs the old FGpts-target result of +0.052 — the significant XBH finding was an artifact of the retired FGpts blend. XBH component itself already cut from article display.
3. **(Optional, discuss w/ user first) K/BB park lens = a Part III article, not part of iPF** — separate PA-level model: outcome K-per-PA and BB-per-PA, batter-season + pitcher-season random intercepts serve as the expected-K/BB baseline (no physics/wOBAcon calibration needed), park term is what survives; drag is a built-in falsification check (ball carry should not predict called strikes). Needs a sibling fetch of PA-ending events (~2M rows, reuse fetch_statcast_bbe chunk framework — the BBE store excludes K/BB by construction). External check: Savant `index_so` in the park-factors leaderboard payload. Expected findings: K effects small + concentrated (Coors, visibility/batter's-eye), BB ≈ 0. Grounded in the existing park-adjusted Stuff+ article (Coors Stuff+ delta −3.9, Loc+ only −0.4; stuff→K link is the k% predictor article). iPF stays contact-only; article's "iPF measures contact only" limitation already flags this.
