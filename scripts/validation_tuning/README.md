# validation_tuning

Research and validation scripts. Nothing in here runs in the pipeline — these
are the files that answer "should we change the model?", and most of them
answered **no**.

Every script's own header carries its question, method, and result. This file
is the index: what's here, what runs, and what will bite you.

Last audited 2026-07-30.

---

## Read this first

**1. Most of these questions are CLOSED.** Before re-running something to
"see if we should change the weights", read that file's header. A verdict block
means it was tested against pre-registered rules and rejected. Re-litigating it
without a new input signal is wasted work.

**2. Three scripts change the product. The rest are read-only research.**

| Script | Effect |
|---|---|
| `derive_sp_skillz_weights_v2.R` | Emits the **live** SP Skillz weights. `set.seed(20260715)` is load-bearing. |
| `derive_park_factor_weights.R` / `_v2.R` | Write into `data/processed/park_factors/`. |

Everything else prints to stdout or writes into a research directory.

**3. Two scripts do not run as committed.** See "Broken" below.

**4. `compare_streamonator_*.R` are the HITTER tool, not the SP one.** They sit
next to the SP scripts and share a naming convention with them, which is the
single most confusing thing about this folder. The SP-side comparison is
`streamonator_v1_v2_comparison.R`.

---

## Traps that have already cost real time

**The stale GSM column.** `derive_streamonator_weights*.R` write a
`good_start_score` column into `starts_YYYY.csv` using the OLD 0–5 definition
(includes Win, flat ER≤3, WHIP≤1.20). The authoritative GSM is **0–4**: no Win,
sliding ER cap, WHIP ≤ 1.18. Source of truth is fbb-tools
`R/modules/leaderboards/mod_gsm.R`. Downstream scripts recompute it and ignore
the stored column. **Anything new reading those CSVs must do the same.**

**Season-final scoring is hindsight.** The `starts_YYYY.csv` lineage scores an
April start with September skill. Fine for *ranking weight combinations*, wrong
for quoting tool performance. Measured inflation: success start-minus-sit +31.2
pts season-final vs +27.3 point-in-time. The honest backtest is fbb-tools
`scripts/build_stream_calibration.R` → `stream_calib_starts_<season>.csv`.

**Integer weights, then divide.** Rebuild a shipped score as
`(6*a + 3*b + 1*c)/10`, never `0.6*a + 0.3*b + 0.1*c`. Those decimal literals
aren't exact in binary and 74 of 18,691 starts land the other side of
`round(x, 1)` — enough to move starts across the 95/105 bucket edges.

**Rank players, not games.** `frank(-proj, by=season)` over game-level rows
selects ~150 *games*, which is 1–2 players. Take a player-level unique rank
first. This produced plausible-looking wrong numbers before it was caught
(platoon read ×1.35, park ×0.99).

**FanGraphs BBE CSVs: no `fileEncoding="UTF-8-BOM"`.** It corrupts the parse.
Read plain, then `names(d)[1] <- "Season"`. Same trap on the Savant arsenal CSV
(`names(d)[1] <- "name"`).

---

## Broken

| Script | Problem | Fix |
|---|---|---|
| `compare_streamonator_extended.R` | Writes its panel cache to a session scratchpad (`b1414f12…`) that no longer exists | Repoint `SCRATCH` at something durable, e.g. `data/processed/hitter_stream_eval` |
| `analyze_topk_sweep.R` | Reads `ext_panel.rds` from that same dead path, and does not build it | Fix both, then run `compare_streamonator_extended.R` first |

---

## External dependencies

Reads `~/Downloads` (manual CSV drops — will fail on a clean machine):
`plot_miss_scatters.R`, `wide_corr_stability.R`,
`streamonator_gb_defense_interaction.R`, `streamonator_home_road_park.R`

Hardcoded absolute `base_dir` (fine on this machine, not portable): 18 scripts,
grep for `"/Users/ckaufman` to list them.

Point-in-time backtest data lives in the **fbb-tools** repo, not here:
`streamonator_coinflip_reweight.R` reads it, with `--calib-dir` to override.

---

## Index

### SP Streamonator — weights and buckets
| Script | Question | Verdict |
|---|---|---|
| `derive_streamonator_weights.R` | 2026 weight grid | builder |
| `derive_streamonator_weights_2025.R` | 2025 weight grid | builder |
| `derive_streamonator_weights_multi.R` | 2021–24 weight grids | builder (~12k boxscore fetches cold) |
| `bucket_analysis.R` | Bucket validation, recomputes authoritative GSM | **the real bucket validation** |
| `derive_streamonator_pf_lenses.R` | Phase 0 enrich: 4 iPF lenses + HR + GSM | builder for the ladder |
| `streamonator_lens_ladder.R` | Should the PF slot use a different lens or a mix? | **all rungs REJECTED** |
| `streamonator_coinflip_reweight.R` | Does the 95–105 band want different weights? | **REJECTED ×4 metrics** |
| `streamonator_v1_v2_comparison.R` | Did 6:3:1 survive SP Skillz v1→v2? | *no verdict recorded* |

### SP Streamonator — defense, shape, park
| Script | Question | Verdict |
|---|---|---|
| `streamonator_defense_analysis.R` | Does own-team defense earn a weight? | **no** — orthogonal but weight stays 1 |
| `streamonator_defense_bucket_compare.R` | Does defense move bucket accuracy? | **no** — Start bucket flat at ~69.8% |
| `streamonator_defense_metric_decomp.R` | Should OAA/DRS/UZR be weighted 1:1:1? | **yes, keep 1:1:1** |
| `streamonator_gb_defense_interaction.R` | Does defense matter more for GB arms? | **yes but tiny** — settled |
| `streamonator_shape_diagnostic.R` | Are the components linear, or do tails deviate? | **linear** — outlier-emphasis rejected |
| `streamonator_home_road_park.R` | Why do arms post bad ERAs in bad parks? | park drives splits; **validates 6:3:1** |

### SP Skillz
| Script | Question | Verdict |
|---|---|---|
| `derive_sp_skillz_weights_v2.R` | The production weight derivation | **ships the live weights** |
| `sp_skillz_empirical_validation.R` | Exploratory metric screen (~1,300 lines) | predecessor to v2 |
| `sp_skillz_v1_v2_comparison.R` | v1 vs v2 on completed seasons | *no verdict recorded* |
| `sp_skillz_2026_ytd_comparison.R` | v1 vs v2 on the live season | face-validity look |

### Arsenal / pitch mix
| Script | Question | Verdict |
|---|---|---|
| `arsenal_features_phase1.R` | Does arsenal shape add signal? | breadth buys **innings, not rates**; 2-pitch −4.8pp |
| `arsenal_features_phase1b.R` | Dose-response, family variety, joint model | **cliff not slope** (+4.0pp at 2→3) |
| `arsenal_features_phase1c.R` | Savant SoT rebuild + confirmatory ladder | **ADOPT n10 upstream; REJECT streamonator knob** |
| `arsenal_phase2_change_study.R` | Is a mix change a "stats are stale" trigger? | **no** — mild positive adaptation signal |
| `arsenal_tracker_prototype.R` | Does the windowed change detector work? | validated; real module shipped |

### Park factors
| Script | Question | Verdict |
|---|---|---|
| `derive_park_factor_weights.R` | Blend reconstructing wOBAcon residual | v1 — target guarantees BACON wins |
| `derive_park_factor_weights_v2.R` | Blend against the fantasy-points target | prefer this one |
| `analyze_humidor_effect.R` | Does the 2022 humidor justify era splits? | **no era splits** — closed 2026-07-02 |

### Hitter Streamonator
| Script | Question | Verdict |
|---|---|---|
| `analyze_baseline_discrimination.R` | Honest pick-accuracy bar | **~coin flip** among similar hitters |
| `analyze_category_discrimination.R` | Fair cross-category comparison | all cats barely beat 50% |
| `analyze_clean_r2.R` | Fix the projection-scaling leak | **8.7%** of weekly variance, not 6.7% |
| `analyze_pickacc_ci.R` | CI machinery / noise floor | built + verified; reuse it |
| `qa_spine_reconcile.R` | Does the spine match MLB official? | **exact, 0.00% diff** |
| `test_shrunk_stacking.R` | Reliability-shrunk stacking, 2022–25 | wash — underpowered, not refuted |
| `test_extended_overall.R` | Same over 2016–2025, 9 folds | **shrunk 61% vs plain 57%** — shipped |
| `test_extended_overall_handsplit.R` | Same with hand-split park | holds at 61% |
| `compare_streamonator_baselines.R` | Four-way baseline comparison, v1 | superseded by v2 |
| `compare_streamonator_v2.R` | Four-way, improvement queue | **the headline numbers** |
| `compare_streamonator_extended.R` | Article-grade single-window pass | article numbers — **broken** |
| `analyze_topk_sweep.R` | Is K=5 cherry-picked? | **broken**, no verdict |

### Bat speed / swing map
`yoy_stability.R`, `wide_corr_stability.R`, `plot_miss_scatters.R` — article
research on `data/processed/swing_map/`. No verdicts recorded.
