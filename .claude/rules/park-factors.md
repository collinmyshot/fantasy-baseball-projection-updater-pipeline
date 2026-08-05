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
  - "scripts/park_factors/fetch_statcast_pa.R"
  - "scripts/park_factors/verify_pa_store.R"
  - "scripts/park_factors/build_park_k_factors.R"
  - "scripts/park_factors/validate_park_k_factors.R"
  - "scripts/park_factors/fetch_savant_park_factors.R"
  - "scripts/park_factors/check_month_curve.R"
  - "scripts/analyze_humidor_effect.R"
  - "scripts/log_park_factor_build_checkpoints.sh"
  - "scripts/log_xba_backfill_checkpoints.sh"
---

# Park Factors Context

**iPF (isolated Park Factor)**: hierarchical random-effects model on Statcast BBE data, built to isolate the park's own effect from players, defense, ball drag, and seasonal patterns. As of July 2026 the Overall factor is the main model's wOBAcon park effect taken DIRECTLY (no component blend; the old 45/35/20 BACON/HR/XBH composite is retired). BACON, HR, and Carry are displayed as component lenses.

**Publish path (only one):** copy the five **`_with_id`** clean CSVs (`park_factors_savant_style_clean_2026_with_id.csv` + `_1H_with_id` + `_2H_with_id` + `_LHB_with_id` + `_RHB_with_id`) plus `park_factor_era_archive.csv` to `/Users/ckaufman/Documents/fbb-tools-repo/data/park_factors/` and deploy the fbb-tools Shiny app via local rsconnect. As of 2026-07 the `_with_id` file is the SINGLE canonical source read by BOTH the PF leaderboard (`mod_park_factors.R`) AND the SP Streamonator/Outlook (`stream_load_pf`) — do NOT copy the non-`_with_id` display CSVs to fbb-tools (they are monorepo-only article/comparison inputs; copying them instead of the `_with_id` set was the old drift bug). `stuff_plus_by_park.csv` also lives in fbb-tools `data/park_factors/` (copy only when the Stuff+ pipeline regenerates it). The Google Sheets export is retired and its code removed — do not resurrect it.

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
- `park_factors_savant_style_clean_2026_with_id.csv` (+ `_1H_with_id`/`_2H_with_id`/`_LHB_with_id`/`_RHB_with_id`) — the canonical outputs consumed by the fbb-tools app (PF leaderboard views and SP Streamonator/Outlook). The non-`_with_id` variants are monorepo-only (article/comparison inputs) — NOT copied to fbb-tools
- `park_factors_by_hand.csv` (+ `park_factors_{bacon,hr,xbh,distance}_by_hand.csv`) — batter-side park effects (park effect + park-hand deviation; league `hand` fixed effect keeps platoon dynamics out). Hand cells shrink toward the park's overall effect where thin
- `park_factor_era_archive.csv` — all eras (current + historical, min 800 BBE) on the CURRENT 30-park scale, with LHB/RHB overall columns; built by `scripts/build_park_factor_archive.R` after clean_2026. Copied to fbb-tools for the Era Archive view
- `pf_build_history.csv` — append-only ledger: each build's published archive rows keyed by build_date + data_through. Never rewrite old rows; the archive script appends automatically. Closed eras stay live in the model (data frozen, estimate re-shrunk each build) — the ledger is what makes past published numbers retrievable
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

## AUGUST 2026 REFRESH (in progress, started 2026-08-03)
User-approved scope: all three July queue items, K% as a new column in the existing views, ONE combined deploy, build locally and review results BEFORE any deploy. 2026 team defense deferred to the offseason (still the `defense_missing` indicator).

Items 1 and 2 are implemented:
1. **Month-bucket seasonal fixed effect** — DONE. `month_grp` (mar_apr, may, jun, jul, aug, sep_oct) replaces the league-wide `half` fixed effect in fit_park_factor_model + fit_component_model; `month_grp` is built in prepare_model_data next to `half`. Park×half random effects (`park_era_half_id`) and the 1H/2H tabs are untouched. Pilot basis (scripts/park_factors/pilot_month_effects.R): month structure beyond half is real (F=50.3, p≈2e-42), park effects unaffected (correlation 0.99994) — hygiene, not a ranking change. Sanity check the fitted month curve against Tango's ~10°F = +10% HR rule.
2. **Points component removed** — DONE. `points` spec deleted from component_specs, `pts_resid`/`pts_on_contact`/`xpts_contact` deleted from R/park_factors.R, and the points outputs + run_metadata keys are gone. Confirmed dead beforehand: XBH re-derived against the SHIPPED target adds only +0.0012 R² (p=0.17).
3. **K% lens** — a PA-level sibling model, NOT part of Overall. See below.

### Park K% lens (new in August 2026)
iPF measures contact only, so K% needs its own PA-level data and model.
- `scripts/park_factors/fetch_statcast_pa.R` — sibling of fetch_statcast_bbe.R keeping the PA-ending rows (`events` non-empty) with `p_throws` added. Same chunk framework, same inclusive-date and freshness semantics. Store lives in the MAIN checkout (`data/raw/statcast_pa_store.csv`, gitignored) so it survives worktree cleanup. Validated on 2024-06-12 against the existing 2021-2025 pitch store: 1,147 PAs, 246 K, 87 BB, zero rows differing in either direction.
- `scripts/park_factors/build_park_k_factors.R` — K-per-PA linear probability model (lmer) with park_era / park×half / park×hand / batter-season / pitcher-season / fielding-team-season / batting-team-season random effects; month_grp + hand + platoon-matchup + drag fixed effects. BB-per-PA sidecar as a falsification target. Writes an explicit event taxonomy (`k_event_taxonomy.csv`) rather than silently classifying; unrecognized events are excluded AND warned about.
- `scripts/park_factors/verify_pa_store.R` — verifies the finished PA store against `data/raw/statcast_pitches/statcast_pitches_2021-2025.rds`, an independent pull of the same source from 2026-07-13 (per-season PA/K/BB counts + exact game_pk+at_bat_number reconciliation over 2021-2025), plus wide plausibility bands on the non-overlap seasons.
- `scripts/park_factors/validate_park_k_factors.R` — three falsification checks (drag must not predict K, BB effect ≈ 0, wall-distance era events must not move K) plus external agreement vs Savant `index_so`. The drag check reports a t-value and the drag coverage share, because a low-coverage build can show a slope that is really a season-level artifact.
- `scripts/park_factors/fetch_savant_park_factors.R` — Savant's own park factor leaderboard (embedded `var data = [...]`, cache-buster required). Carries `index_so`/`index_bb`, keyed by venue_id which joins to `venue_<id>`. NOTE Savant's payload is a 3-year rolling window; it records its own `year_range`.
- `k_idx_100` merges into the five `_with_id` CSVs via build_park_factor_clean_2026.R, standardized with `std_index` like every other lens. The merge degrades to NA when the K outputs are absent, so the contact-only chain still runs standalone. `build_park_factor_archive.R` joins `park_factors_k_overall.csv` by era and emits `k_idx` (current-30 scale) + `k_pct_pts` (raw K% points).

**Display decisions (user, 2026-08-04):** K% ships as a column in ALL views including 1H/2H. The 1H and 2H numbers are identical by construction (park-by-half K variance is exactly zero, a singular fit) and the user considers that identity itself informative, so it is shown rather than suppressed. K is colour-INVERTED in the app (`pf_created_row_js(color_cols, invert_cols)` maps `val -> 200 - val` for K) because more strikeouts favours the pitcher, so a high K must read navy like a low Overall. K is also in the Era Archive view.

### ✍️ ARTICLE NOTE: batter approach is a confound for park K% (locked in 2026-08-04)
Use this in the K% article as a stated limitation. Do NOT expand it into a full decomposition (user decision, 2026-08-04: worth naming, not worth digging further).

Article-ready wording (house style: no dashes, no contractions):

> A park factor for strikeouts cannot fully separate what the ball does from what the hitter chooses to do. At Coors Field, where contact is rewarded more than anywhere else, hitters swing 2.2 percent more often and chase 2.1 percent less often than they do elsewhere. That is what hunting contact looks like, and some part of the park's strikeout suppression is therefore a decision rather than an effect of the air.

Provenance: descriptive split of 3,567,640 pitches, 2021-2025 (`data/raw/statcast_pitches/statcast_pitches_2021-2025.rds`), Coors vs all other parks. Coors swing rate 0.4857 vs 0.4753 elsewhere (+2.2% relative); chase rate on out-of-zone pitches 0.2772 vs 0.2832 (−2.1% relative); whiff per swing 0.2274 vs 0.2351 (−3.3% relative, the contact/physics channel). These are RAW rates and are NOT controlled for batter or pitcher identity, so quote them as descriptive context only, never as model output.

Supporting evidence that the effect is mostly environmental rather than learned: pooling 2015-2026 by career, visiting hitters average 51 career Coors PAs against Rockies hitters' 551 (about eleven times less exposure) yet still show 79 percent of the strikeout suppression (−3.21 vs −4.08 K% points, raw within-batter deltas, not pitcher-controlled). Single-season Coors PA tops out at 357, so any per-season framing of those exposure numbers is wrong.

**K% findings (2026-08-04 build, 1,963,426 PAs):** park K spread runs Coors −2.03 K% points (idx 76.9) to T-Mobile +2.04 (idx 121.2), roughly 20.1% to 24.2% against a 22.15% baseline. K is nearly orthogonal to Overall (R² = 0.058), so it is genuinely new information. Falsification: BB park SD is 0.12 BB% points vs K 0.80 (ratio 6.6), so parks move strikeouts far more than walks, ruling out umpire/framing artifacts. glmer vs LPM park effects R² = 0.9993. Savant `index_so` agreement R² = 0.685 with our spread narrower (SD 4.69 vs 5.94) as expected since Savant does not control for batter/pitcher. Drag is a CONTROL not a falsification instrument (see script header).

⚠️ **The displayed index is a z-score index, not a ratio** (`std_index` = 100 + 10*z over the 30 current parks). Coors 130.8 = 3.08 SD, not +30% offense. Savant's indexes ARE ratios, so compare with R², never by reading levels side by side.
