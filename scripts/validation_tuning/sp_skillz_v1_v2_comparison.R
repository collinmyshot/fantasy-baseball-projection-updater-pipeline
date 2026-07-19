#!/usr/bin/env Rscript
# SP Skillz v1 vs v2 — Predictive Comparison + Biggest Movers
# All output via cat() so it actually prints in the console

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
})

base_dir <- "/Users/ckaufman/Documents/New project"
data_dir <- file.path(base_dir, "data/processed/sp_skillz_v2")

# ── Load cached data ─────────────────────────────────────────────────
v2_full <- read_csv(file.path(data_dir, "sp_skillz_v2_full_seasons_2021_2025.csv"), show_col_types = FALSE)
v1_full <- read_csv(file.path(data_dir, "sp_skillz_v1_live_full_seasons_2021_2025.csv"), show_col_types = FALSE)
v2_1h   <- read_csv(file.path(data_dir, "sp_skillz_v2_first_half_2021_2025.csv"), show_col_types = FALSE)
v1_1h   <- read_csv(file.path(data_dir, "sp_skillz_v1_live_first_half_2021_2025.csv"), show_col_types = FALSE)

# Standardize column names
v1_full <- v1_full %>% rename(v1_score_stab_full = v1_score_stab, v1_rank_stab_full = v1_rank_stab)
v2_full <- v2_full %>% rename(v2_score_stab_full = sp_skillz_score_stabilized, v2_rank_stab_full = sp_skillz_rank_stabilized)
v1_1h   <- v1_1h %>% rename(v1_score_stab_1h = v1_score_stab, v1_rank_stab_1h = v1_rank_stab)
v2_1h   <- v2_1h %>% rename(v2_score_stab_1h = sp_skillz_score_stabilized, v2_rank_stab_1h = sp_skillz_rank_stabilized)

# ══════════════════════════════════════════════════════════════════════
# PART 1: YoY Prediction (full-season score → next-year outcomes)
# ══════════════════════════════════════════════════════════════════════

cat("\n")
cat("======================================================================\n")
cat("PART 1: YoY PREDICTION — Full-Season Score → Next-Year Outcomes\n")
cat("======================================================================\n\n")

# Build YoY pairs: year N score → year N+1 outcomes
targets_cols <- c("k_pct", "siera", "era", "whip")

# Get targets from v2_full (has all outcome columns)
outcomes <- v2_full %>%
  select(player_id, season, k_pct, siera, era, whip) %>%
  mutate(season = season - 1L)  # shift back so we can join on base year

# Join v1 and v2 scores with next-year outcomes
v1_yoy <- v1_full %>%
  select(player_id, season, v1_score_stab_full) %>%
  inner_join(outcomes, by = c("player_id", "season"))

v2_yoy <- v2_full %>%
  select(player_id, season, v2_score_stab_full) %>%
  inner_join(outcomes, by = c("player_id", "season"))

# Combined for apples-to-apples (same pitcher-pairs)
yoy <- v1_full %>%
  select(player_id, season, v1_score_stab_full) %>%
  inner_join(
    v2_full %>% select(player_id, season, v2_score_stab_full),
    by = c("player_id", "season")
  ) %>%
  inner_join(outcomes, by = c("player_id", "season"))

cat(sprintf("  YoY pairs (matched v1+v2+next-year outcomes): %d\n\n", nrow(yoy)))

cat("  ┌──────────────┬────────────┬────────────┬────────────┬────────┐\n")
cat("  │ Target       │   v1 R²    │   v2 R²    │   Δ R²     │   n    │\n")
cat("  ├──────────────┼────────────┼────────────┼────────────┼────────┤\n")

for (tgt in targets_cols) {
  ok <- !is.na(yoy$v1_score_stab_full) & !is.na(yoy$v2_score_stab_full) & !is.na(yoy[[tgt]])
  sub <- yoy[ok, ]
  r1 <- cor(sub$v1_score_stab_full, sub[[tgt]])
  r2 <- cor(sub$v2_score_stab_full, sub[[tgt]])
  # Flip sign for lower-is-better targets so R² reflects quality alignment
  r2_v1 <- r1^2
  r2_v2 <- r2^2
  delta <- r2_v2 - r2_v1
  cat(sprintf("  │ next_%-7s │   %.4f   │   %.4f   │  %+.4f   │  %4d  │\n",
              tgt, r2_v1, r2_v2, delta, nrow(sub)))
}

cat("  └──────────────┴────────────┴────────────┴────────────┴────────┘\n")

# ══════════════════════════════════════════════════════════════════════
# PART 2: Early → Full-Season Prediction (1H score → same-year full outcomes)
# ══════════════════════════════════════════════════════════════════════

cat("\n\n")
cat("======================================================================\n")
cat("PART 2: EARLY → FULL-SEASON — 1H Score → Same-Year Full Outcomes\n")
cat("======================================================================\n\n")

# Full-season outcomes for same year
full_outcomes <- v2_full %>%
  select(player_id, season, k_pct, siera, era, whip)

early <- v1_1h %>%
  select(player_id, season, v1_score_stab_1h) %>%
  inner_join(
    v2_1h %>% select(player_id, season, v2_score_stab_1h),
    by = c("player_id", "season")
  ) %>%
  inner_join(full_outcomes, by = c("player_id", "season"))

cat(sprintf("  Early→Full pairs (matched v1+v2+full-season outcomes): %d\n\n", nrow(early)))

cat("  ┌──────────────┬────────────┬────────────┬────────────┬────────┐\n")
cat("  │ Target       │   v1 R²    │   v2 R²    │   Δ R²     │   n    │\n")
cat("  ├──────────────┼────────────┼────────────┼────────────┼────────┤\n")

for (tgt in targets_cols) {
  ok <- !is.na(early$v1_score_stab_1h) & !is.na(early$v2_score_stab_1h) & !is.na(early[[tgt]])
  sub <- early[ok, ]
  r1 <- cor(sub$v1_score_stab_1h, sub[[tgt]])
  r2 <- cor(sub$v2_score_stab_1h, sub[[tgt]])
  r2_v1 <- r1^2
  r2_v2 <- r2^2
  delta <- r2_v2 - r2_v1
  cat(sprintf("  │ %-12s │   %.4f   │   %.4f   │  %+.4f   │  %4d  │\n",
              tgt, r2_v1, r2_v2, delta, nrow(sub)))
}

cat("  └──────────────┴────────────┴────────────┴────────────┴────────┘\n")

# ══════════════════════════════════════════════════════════════════════
# PART 3: Season-by-Season Breakdown (YoY)
# ══════════════════════════════════════════════════════════════════════

cat("\n\n")
cat("======================================================================\n")
cat("PART 3: SEASON-BY-SEASON YoY BREAKDOWN\n")
cat("======================================================================\n\n")

transitions <- sort(unique(yoy$season))

for (base_yr in transitions) {
  sub <- yoy %>% filter(season == base_yr)
  if (nrow(sub) < 20) next
  next_yr <- base_yr + 1
  cat(sprintf("  --- %d → %d (n=%d) ---\n", base_yr, next_yr, nrow(sub)))
  cat("  ┌──────────────┬────────────┬────────────┬────────────┐\n")
  cat("  │ Target       │   v1 R²    │   v2 R²    │   Δ R²     │\n")
  cat("  ├──────────────┼────────────┼────────────┼────────────┤\n")

  for (tgt in targets_cols) {
    ok <- !is.na(sub$v1_score_stab_full) & !is.na(sub$v2_score_stab_full) & !is.na(sub[[tgt]])
    ss <- sub[ok, ]
    if (nrow(ss) < 10) next
    r1 <- cor(ss$v1_score_stab_full, ss[[tgt]])
    r2 <- cor(ss$v2_score_stab_full, ss[[tgt]])
    delta <- r2^2 - r1^2
    cat(sprintf("  │ next_%-7s │   %.4f   │   %.4f   │  %+.4f   │\n",
                tgt, r1^2, r2^2, delta))
  }
  cat("  └──────────────┴────────────┴────────────┴────────────┘\n\n")
}

# ══════════════════════════════════════════════════════════════════════
# PART 4: Biggest Movers (2025 season, most recent)
# ══════════════════════════════════════════════════════════════════════

cat("\n")
cat("======================================================================\n")
cat("PART 4: BIGGEST MOVERS — 2025 Full Season\n")
cat("======================================================================\n\n")

movers <- v1_full %>%
  filter(season == 2025) %>%
  select(player_id, player_name = player_name, team, v1_rank_stab_full) %>%
  inner_join(
    v2_full %>%
      filter(season == 2025) %>%
      select(player_id, v2_rank_stab_full),
    by = "player_id"
  ) %>%
  mutate(rank_change = v1_rank_stab_full - v2_rank_stab_full)  # positive = gained ranks in v2

# Top 10 Gainers (ranked higher in v2)
cat("  TOP 10 GAINERS (ranked higher in 'updated' v2 model)\n")
cat("  ┌─────┬─────────────────────────┬──────┬──────────┬──────────┬──────────┐\n")
cat("  │  #  │ Player                  │ Team │ v1 Rank  │ v2 Rank  │ Δ Rank   │\n")
cat("  ├─────┼─────────────────────────┼──────┼──────────┼──────────┼──────────┤\n")

gainers <- movers %>% arrange(desc(rank_change)) %>% head(10)
for (i in 1:nrow(gainers)) {
  row <- gainers[i, ]
  cat(sprintf("  │ %2d  │ %-23s │ %-4s │   %4d   │   %4d   │  %+5d   │\n",
              i, substr(row$player_name, 1, 23), row$team,
              row$v1_rank_stab_full, row$v2_rank_stab_full, row$rank_change))
}
cat("  └─────┴─────────────────────────┴──────┴──────────┴──────────┴──────────┘\n\n")

# Top 10 Fallers (ranked higher in v1)
cat("  TOP 10 FALLERS (ranked higher in 'live' v1 model)\n")
cat("  ┌─────┬─────────────────────────┬──────┬──────────┬──────────┬──────────┐\n")
cat("  │  #  │ Player                  │ Team │ v1 Rank  │ v2 Rank  │ Δ Rank   │\n")
cat("  ├─────┼─────────────────────────┼──────┼──────────┼──────────┼──────────┤\n")

fallers <- movers %>% arrange(rank_change) %>% head(10)
for (i in 1:nrow(fallers)) {
  row <- fallers[i, ]
  cat(sprintf("  │ %2d  │ %-23s │ %-4s │   %4d   │   %4d   │  %+5d   │\n",
              i, substr(row$player_name, 1, 23), row$team,
              row$v1_rank_stab_full, row$v2_rank_stab_full, row$rank_change))
}
cat("  └─────┴─────────────────────────┴──────┴──────────┴──────────┴──────────┘\n\n")

# Also show 2024 movers for another data point
cat("\n")
cat("======================================================================\n")
cat("PART 4b: BIGGEST MOVERS — 2024 Full Season\n")
cat("======================================================================\n\n")

movers24 <- v1_full %>%
  filter(season == 2024) %>%
  select(player_id, player_name = player_name, team, v1_rank_stab_full) %>%
  inner_join(
    v2_full %>%
      filter(season == 2024) %>%
      select(player_id, v2_rank_stab_full),
    by = "player_id"
  ) %>%
  mutate(rank_change = v1_rank_stab_full - v2_rank_stab_full)

cat("  TOP 10 GAINERS (ranked higher in 'updated' v2 model)\n")
cat("  ┌─────┬─────────────────────────┬──────┬──────────┬──────────┬──────────┐\n")
cat("  │  #  │ Player                  │ Team │ v1 Rank  │ v2 Rank  │ Δ Rank   │\n")
cat("  ├─────┼─────────────────────────┼──────┼──────────┼──────────┼──────────┤\n")

gainers24 <- movers24 %>% arrange(desc(rank_change)) %>% head(10)
for (i in 1:nrow(gainers24)) {
  row <- gainers24[i, ]
  cat(sprintf("  │ %2d  │ %-23s │ %-4s │   %4d   │   %4d   │  %+5d   │\n",
              i, substr(row$player_name, 1, 23), row$team,
              row$v1_rank_stab_full, row$v2_rank_stab_full, row$rank_change))
}
cat("  └─────┴─────────────────────────┴──────┴──────────┴──────────┴──────────┘\n\n")

cat("  TOP 10 FALLERS (ranked higher in 'live' v1 model)\n")
cat("  ┌─────┬─────────────────────────┬──────┬──────────┬──────────┬──────────┐\n")
cat("  │  #  │ Player                  │ Team │ v1 Rank  │ v2 Rank  │ Δ Rank   │\n")
cat("  ├─────┼─────────────────────────┼──────┼──────────┼──────────┼──────────┤\n")

fallers24 <- movers24 %>% arrange(rank_change) %>% head(10)
for (i in 1:nrow(fallers24)) {
  row <- fallers24[i, ]
  cat(sprintf("  │ %2d  │ %-23s │ %-4s │   %4d   │   %4d   │  %+5d   │\n",
              i, substr(row$player_name, 1, 23), row$team,
              row$v1_rank_stab_full, row$v2_rank_stab_full, row$rank_change))
}
cat("  └─────┴─────────────────────────┴──────┴──────────┴──────────┴──────────┘\n\n")

cat("Done.\n")
