# ⛔ DEPRECATED -- do not start new work here

**This repo (`fantasy-baseball-projection-updater-pipeline`) is being retired.
New work goes to `~/Documents/fbb-tools-repo`.**

Nothing here runs on a schedule any more -- the last live automation (the Google
Sheets refresh) was retired 2026-08-15. But the repo is still the **only** copy
of a fair amount of code. Read the "still lives only here" section before
deleting anything or assuming a file has a copy elsewhere.

---

## Why

Park factors were built here and *published* to fbb-tools by hand-copying the
output CSVs. On 2026-08-04 the CSVs were copied and the build code was not, so
fbb-tools spent three weeks serving park factors it could not reproduce: its
build scripts were still May 21 while the data on the site was August.

Two repos made that possible; the hand-copy made it happen. Both are now fixed
in fbb-tools, where the build publishes to the app in code, refuses to overwrite
a newer artifact with an older build, records provenance, and fails the deploy
if the two ever disagree.

---

## What MOVED to fbb-tools (do not edit the copies here)

| here | there |
|---|---|
| `scripts/park_factors/` (24 files) | `scripts/park_factors/` (25 -- plus the new `publish_park_factors.R`) |
| `scripts/validation_tuning/` (43 files) | `scripts/validation_tuning/` |
| `R/park_factors.R` | `R/engines/park_factors.R` |
| `data/processed/park_factors/` | `research/park_factors/` |
| `data/processed/streamonator_weight_analysis/` | `research/streamonator_weight_analysis/` |
| `data/processed/{hitter_game_logs,arsenal_research,sp_skillz_v2,sp_skillz_validation,swing_map}/` | `research/<same>/` |

Research data lives under `research/` there, deliberately: fbb-tools' `deploy.R`
uses an **allowlist** (`appFiles`), and `research/` is not a path it names, so
none of it can ever reach a deploy bundle. Do not "helpfully" move it under
`data/processed/` -- that directory is bundled wholesale, and gitignoring does
not help because the allowlist reads the filesystem, not git.

The raw boxscore cache (`box*.rds`, ~12.6k files / 279 MB) was **not** copied.
It is gitignored there as a re-fetchable cache; its useful extract ships as the
`starts_*.csv` files. The copy in this repo is currently the only one on disk.

**`R/sp_skillz.R` here is STALE** (Apr 7). fbb-tools' `R/engines/sp_skillz.R` is
the live, sweeper-aware version and is ~285 lines ahead. Never copy this one
over that one.

---

## What still lives ONLY here -- deleting this repo loses it

### The Google Sheets draft pipeline (RETIRED 2026-08-15 -- no longer runs)

`.github/workflows/daily-refresh.yml` used to run `scripts/run_pipeline.R` on a
cron covering Dec 24 - Apr 15. **The schedule has been removed.** Only
`workflow_dispatch` remains, so it can be triggered by hand but will never fire
on its own. Retiring it was the last thing keeping this repo operationally live.

The Google Sheet is untouched -- the pipeline only ever wrote to it, so the
hand-entered columns (CK.Rank, DS.Rank, RBLL targets) are as you left them.
If you ever run this manually, note it WRITES to that live sheet with
projections that may be several seasons stale.

The code is kept, not deleted, so the pipeline can be revived. Scripts with no
counterpart in fbb-tools:
`run_pipeline.R`, `fetch_projections.R`, `fetch_nfbc_adp.R`,
`build_pitcher_integration_table.R`, `push_to_google_sheets.R`,
`sync_run_data_tab.R`, `sync_adp_tab.R`, `sync_position_tabs.R`,
`setup_rbell_team_targets.R`, `format_workbook_tabs.R`

### Hitter article research

`build_hitter_article_effects.R`, `build_hitter_article_winrates.R`,
`build_hitter_ghw_calibration.R`, `build_hitter_article_docx.py`,
`build_bat_speed_research.R`, `build_platoon_grids.R`, `build_hitter_map.R`,
`find_similar_hitters.R`, `analyze_article_round3.R`, `validate_swing_map.R`

### Other

`build_ottoneu_points_csv.R`, `calc_sp_skillz.R`, `derive_sp_skillz_weights.R`,
`fetch_team_defense.R`, `fetch_savant_monthly_mix.R`, `init_renv.R`,
`R/fangraphs_projections.R`, `R/gsheets_auth.R`, `R/pipeline_config.R`,
`config/pipeline.yml`, `.claude/rules/`

---

## Rules while this repo still exists

1. **Park factor or Streamonator validation work → fbb-tools.** Not here.
2. **Do not edit the moved files here.** They are a frozen copy. Edits will be
   lost and will re-create the drift this migration removed.
3. **Do not delete this repo** while it is still the only copy of the scripts
   listed above. The Sheets pipeline is retired, so nothing here runs any more,
   but retired is not the same as backed up.
4. Do not re-add a `schedule:` block to any workflow here. If something needs to
   run on a cron, it belongs in fbb-tools.
