#!/usr/bin/env Rscript
# ===========================================================================
# Streamonator Weight Validation: v1+2025PF (baseline) vs v2+2026PF (updated)
# ===========================================================================
# Loads cached starts_YYYY.csv (2021-2025), adds:
#   - v2 SP Skillz index (from cached v2 scores)
#   - 2026 park factor (from rebuilt clean file)
# Runs grid search under both configurations, compares.
# ===========================================================================

suppressPackageStartupMessages(library(dplyr))

CACHE_DIR <- file.path("data", "processed", "streamonator_weight_analysis")
V2_DIR    <- file.path("data", "processed", "sp_skillz_v2")
PF_DIR    <- file.path("data", "processed", "park_factors")
SEASONS   <- 2021:2025

# ══════════════════════════════════════════════════════════════════════
# LOAD DATA
# ══════════════════════════════════════════════════════════════════════

cat("======================================================================\n")
cat("LOADING DATA\n")
cat("======================================================================\n\n")

# ── 1. Load cached starts ────────────────────────────────────────────
all_starts <- do.call(rbind, lapply(SEASONS, function(yr) {
  f <- file.path(CACHE_DIR, sprintf("starts_%d.csv", yr))
  if (!file.exists(f)) { cat(sprintf("  MISSING: %s\n", f)); return(NULL) }
  df <- read.csv(f, stringsAsFactors = FALSE)
  df$season <- yr
  cat(sprintf("  %d: %d starts\n", yr, nrow(df)))
  df
}))
cat(sprintf("  Combined: %d starts\n\n", nrow(all_starts)))

# ── 2. Recompute GSM score (0-4, no Win, WHIP<=1.18, sliding ER) ────
all_starts$whip <- ifelse(
  !is.na(all_starts$ip) & all_starts$ip > 0,
  (all_starts$h + all_starts$bb) / all_starts$ip, Inf)

all_starts$ip_ok <- as.integer(!is.na(all_starts$ip) & all_starts$ip >= 5.0)
all_starts$k_ok  <- as.integer(!is.na(all_starts$k) & !is.na(all_starts$ip) &
                                 all_starts$k >= (floor(all_starts$ip) - 1))
all_starts$er_ok <- as.integer(!is.na(all_starts$er) & !is.na(all_starts$ip) & (
  (all_starts$ip >= 6.0                        & all_starts$er <= 2) |
  (all_starts$ip >= 5.0 & all_starts$ip < 6.0  & all_starts$er <= 3) |
  (all_starts$ip >= 4.0 & all_starts$ip < 5.0  & all_starts$er <= 2) |
  (all_starts$ip <  4.0                        & all_starts$er <= 1)
))
all_starts$whip_ok <- as.integer(!is.na(all_starts$whip) & all_starts$whip <= 1.18)
all_starts$good_start_score <- all_starts$ip_ok + all_starts$k_ok + all_starts$er_ok + all_starts$whip_ok

cat("  GSM score distribution:\n")
tbl <- table(all_starts$good_start_score)
for (sc in names(tbl)) {
  cat(sprintf("    %s: %d (%.1f%%)\n", sc, tbl[[sc]], 100 * tbl[[sc]] / nrow(all_starts)))
}
cat(sprintf("  Good (3-4): %.1f%%\n\n",
            100 * mean(all_starts$good_start_score >= 3, na.rm = TRUE)))

# ── 3. Add v2 SP Skillz index ────────────────────────────────────────
cat("  Adding v2 SP Skillz index...\n")
v2_scores <- read.csv(file.path(V2_DIR, "sp_skillz_v2_full_seasons_2021_2025.csv"),
                       stringsAsFactors = FALSE)

# Compute v2 index per season (100-centered, ±10 = 1 SD)
v2_idx <- do.call(rbind, lapply(split(v2_scores, v2_scores$season), function(df) {
  mu <- mean(df$sp_skillz_score_stabilized, na.rm = TRUE)
  sigma <- sd(df$sp_skillz_score_stabilized, na.rm = TRUE)
  if (is.na(sigma) || sigma == 0) {
    df$v2_sp_skillz_index <- 100
  } else {
    df$v2_sp_skillz_index <- round(100 + (df$sp_skillz_score_stabilized - mu) / sigma * 10, 1)
  }
  df[, c("player_id", "season", "v2_sp_skillz_index")]
}))

# Match to starts by pitcher_id + season
all_starts <- merge(all_starts, v2_idx,
                     by.x = c("pitcher_id", "season"),
                     by.y = c("player_id", "season"),
                     all.x = TRUE)

cat(sprintf("    v2 index matched: %d / %d starts (%.1f%%)\n",
            sum(!is.na(all_starts$v2_sp_skillz_index)), nrow(all_starts),
            100 * mean(!is.na(all_starts$v2_sp_skillz_index))))

# ── 4. Add 2026 park factors ─────────────────────────────────────────
cat("  Adding 2026 park factors...\n")
pf_2026 <- read.csv(file.path(PF_DIR, "park_factors_savant_style_clean_2026_with_id.csv"),
                     stringsAsFactors = FALSE)

# Normalize team names to match starts format
norm_team <- function(x) {
  x <- toupper(trimws(as.character(x)))
  mapped <- c(KC = "KCR", ARI = "AZ", TB = "TBR", OAK = "ATH", WAS = "WSH", WSN = "WSH")
  hit <- x %in% names(mapped)
  x[hit] <- mapped[x[hit]]
  x
}

pf_2026$team_key <- norm_team(pf_2026$team_norm)
pf_lookup <- setNames(pf_2026$overall_pf_idx_100, pf_2026$team_key)

# Park is determined by home team
park_team <- ifelse(all_starts$home_away == "H",
                    norm_team(all_starts$pitcher_team),
                    norm_team(all_starts$opponent_team))

all_starts$park_factor_raw_2026 <- as.numeric(pf_lookup[park_team])
all_starts$park_factor_inv_2026 <- ifelse(
  is.na(all_starts$park_factor_raw_2026), NA_real_,
  200 - all_starts$park_factor_raw_2026)

cat(sprintf("    2026 PF matched: %d / %d starts (%.1f%%)\n\n",
            sum(!is.na(all_starts$park_factor_raw_2026)), nrow(all_starts),
            100 * mean(!is.na(all_starts$park_factor_raw_2026))))

# ══════════════════════════════════════════════════════════════════════
# GRID SEARCH SETUP
# ══════════════════════════════════════════════════════════════════════

GRID <- expand.grid(
  w_sp = seq(0, 1, by = 0.1),
  w_tr = seq(0, 1, by = 0.1),
  w_pf = seq(0, 1, by = 0.1)
)
GRID <- GRID[abs(rowSums(GRID) - 1) < 1e-9, , drop = FALSE]
rownames(GRID) <- NULL

run_grid <- function(df, sp_col, pf_inv_col, label) {
  outcome <- df$good_start_score
  tr_col  <- "team_rater_inv"

  results <- do.call(rbind, lapply(seq_len(nrow(GRID)), function(j) {
    w_sp <- GRID$w_sp[j]; w_tr <- GRID$w_tr[j]; w_pf <- GRID$w_pf[j]
    scores <- vapply(seq_len(nrow(df)), function(i) {
      vals <- c(df[[sp_col]][i], df[[tr_col]][i], df[[pf_inv_col]][i])
      wts  <- c(w_sp, w_tr, w_pf)
      ok   <- !is.na(vals) & wts > 0
      if (!any(ok)) return(NA_real_)
      sum(vals[ok] * wts[ok]) / sum(wts[ok])
    }, numeric(1))
    valid <- !is.na(scores) & !is.na(outcome)
    if (sum(valid) < 10L) return(NULL)
    rho     <- suppressWarnings(cor(scores[valid], outcome[valid], method = "spearman"))
    top_cut <- quantile(scores[valid], 0.75, na.rm = TRUE)
    pct_ge3 <- mean(outcome[valid & scores >= top_cut] >= 3, na.rm = TRUE)
    data.frame(config = label, w_sp = w_sp, w_tr = w_tr, w_pf = w_pf,
               spearman = round(rho, 4), pct_ge3 = round(pct_ge3, 4),
               n = sum(valid), stringsAsFactors = FALSE)
  }))
  results[order(-results$spearman), , drop = FALSE]
}

# ══════════════════════════════════════════════════════════════════════
# RUN GRID SEARCHES
# ══════════════════════════════════════════════════════════════════════

cat("======================================================================\n")
cat("GRID SEARCH: BASELINE (v1 SP Skillz + 2025 Park Factors)\n")
cat("======================================================================\n\n")

grid_baseline <- run_grid(all_starts, "sp_skillz_index", "park_factor_inv", "v1+PF2025")
cat("  Top 10 weight combos:\n\n")
cat(sprintf("  %5s  %5s  %5s  %8s  %8s  %7s\n",
            "w_sp", "w_tr", "w_pf", "Spearman", "pct_ge3", "N"))
cat(sprintf("  %s\n", strrep("-", 50)))
for (i in 1:min(10, nrow(grid_baseline))) {
  r <- grid_baseline[i, ]
  cat(sprintf("  %5.1f  %5.1f  %5.1f  %8.4f  %8.4f  %7d\n",
              r$w_sp, r$w_tr, r$w_pf, r$spearman, r$pct_ge3, r$n))
}

# Reference 6:3:1
ref_b <- grid_baseline[abs(grid_baseline$w_sp - 0.6) < 0.05 &
                          abs(grid_baseline$w_tr - 0.3) < 0.05 &
                          abs(grid_baseline$w_pf - 0.1) < 0.05, ]
cat(sprintf("\n  6:3:1 reference: Spearman=%.4f, pct_ge3=%.4f\n",
            ref_b$spearman[1], ref_b$pct_ge3[1]))

cat("\n\n")
cat("======================================================================\n")
cat("GRID SEARCH: UPDATED (v2 SP Skillz + 2026 Park Factors)\n")
cat("======================================================================\n\n")

# Filter to rows where v2 index is available
v2_valid <- all_starts[!is.na(all_starts$v2_sp_skillz_index), ]
cat(sprintf("  Starts with v2 SP Skillz: %d / %d (%.1f%%)\n\n",
            nrow(v2_valid), nrow(all_starts),
            100 * nrow(v2_valid) / nrow(all_starts)))

grid_updated <- run_grid(v2_valid, "v2_sp_skillz_index", "park_factor_inv_2026", "v2+PF2026")
cat("  Top 10 weight combos:\n\n")
cat(sprintf("  %5s  %5s  %5s  %8s  %8s  %7s\n",
            "w_sp", "w_tr", "w_pf", "Spearman", "pct_ge3", "N"))
cat(sprintf("  %s\n", strrep("-", 50)))
for (i in 1:min(10, nrow(grid_updated))) {
  r <- grid_updated[i, ]
  cat(sprintf("  %5.1f  %5.1f  %5.1f  %8.4f  %8.4f  %7d\n",
              r$w_sp, r$w_tr, r$w_pf, r$spearman, r$pct_ge3, r$n))
}

ref_u <- grid_updated[abs(grid_updated$w_sp - 0.6) < 0.05 &
                         abs(grid_updated$w_tr - 0.3) < 0.05 &
                         abs(grid_updated$w_pf - 0.1) < 0.05, ]
cat(sprintf("\n  6:3:1 reference: Spearman=%.4f, pct_ge3=%.4f\n",
            ref_u$spearman[1], ref_u$pct_ge3[1]))

# ── Also run v1+2025PF on same subset for apples-to-apples ──────────
cat("\n\n")
cat("======================================================================\n")
cat("APPLES-TO-APPLES: v1+2025PF on same subset as v2\n")
cat("======================================================================\n\n")

grid_baseline_matched <- run_grid(v2_valid, "sp_skillz_index", "park_factor_inv", "v1+PF2025 (matched)")
cat("  Top 10 weight combos:\n\n")
cat(sprintf("  %5s  %5s  %5s  %8s  %8s  %7s\n",
            "w_sp", "w_tr", "w_pf", "Spearman", "pct_ge3", "N"))
cat(sprintf("  %s\n", strrep("-", 50)))
for (i in 1:min(10, nrow(grid_baseline_matched))) {
  r <- grid_baseline_matched[i, ]
  cat(sprintf("  %5.1f  %5.1f  %5.1f  %8.4f  %8.4f  %7d\n",
              r$w_sp, r$w_tr, r$w_pf, r$spearman, r$pct_ge3, r$n))
}

ref_bm <- grid_baseline_matched[abs(grid_baseline_matched$w_sp - 0.6) < 0.05 &
                                    abs(grid_baseline_matched$w_tr - 0.3) < 0.05 &
                                    abs(grid_baseline_matched$w_pf - 0.1) < 0.05, ]
cat(sprintf("\n  6:3:1 reference: Spearman=%.4f, pct_ge3=%.4f\n",
            ref_bm$spearman[1], ref_bm$pct_ge3[1]))

# ══════════════════════════════════════════════════════════════════════
# SUMMARY COMPARISON
# ══════════════════════════════════════════════════════════════════════

cat("\n\n")
cat("======================================================================\n")
cat("SUMMARY COMPARISON\n")
cat("======================================================================\n\n")

best_b <- grid_baseline[1, ]
best_bm <- grid_baseline_matched[1, ]
best_u <- grid_updated[1, ]

cat("  ┌─────────────────────────┬────────────────────┬──────────┬──────────┬────────┐\n")
cat("  │ Configuration           │ Best Weights       │ Spearman │ pct_ge3  │   N    │\n")
cat("  ├─────────────────────────┼────────────────────┼──────────┼──────────┼────────┤\n")
cat(sprintf("  │ v1+PF2025 (all starts)  │ %.1f/%.1f/%.1f          │  %.4f  │  %.4f  │ %5d  │\n",
            best_b$w_sp, best_b$w_tr, best_b$w_pf, best_b$spearman, best_b$pct_ge3, best_b$n))
cat(sprintf("  │ v1+PF2025 (matched sub) │ %.1f/%.1f/%.1f          │  %.4f  │  %.4f  │ %5d  │\n",
            best_bm$w_sp, best_bm$w_tr, best_bm$w_pf, best_bm$spearman, best_bm$pct_ge3, best_bm$n))
cat(sprintf("  │ v2+PF2026 (matched sub) │ %.1f/%.1f/%.1f          │  %.4f  │  %.4f  │ %5d  │\n",
            best_u$w_sp, best_u$w_tr, best_u$w_pf, best_u$spearman, best_u$pct_ge3, best_u$n))
cat("  └─────────────────────────┴────────────────────┴──────────┴──────────┴────────┘\n\n")

cat("  At 6:3:1 weights:\n")
cat("  ┌─────────────────────────┬──────────┬──────────┬──────────┐\n")
cat("  │ Configuration           │ Spearman │ pct_ge3  │ Δ vs v1  │\n")
cat("  ├─────────────────────────┼──────────┼──────────┼──────────┤\n")
cat(sprintf("  │ v1+PF2025 (matched sub) │  %.4f  │  %.4f  │    --    │\n",
            ref_bm$spearman[1], ref_bm$pct_ge3[1]))
cat(sprintf("  │ v2+PF2026 (matched sub) │  %.4f  │  %.4f  │ %+.4f  │\n",
            ref_u$spearman[1], ref_u$pct_ge3[1],
            ref_u$spearman[1] - ref_bm$spearman[1]))
cat("  └─────────────────────────┴──────────┴──────────┴──────────┘\n\n")

# ── Season-by-season at 6:3:1 ────────────────────────────────────────
cat("  Season-by-season at 6:3:1 (matched subset):\n\n")
cat(sprintf("  %-8s  %8s  %8s  %8s  %7s\n",
            "Season", "v1+PF25", "v2+PF26", "Delta", "N"))
cat(sprintf("  %s\n", strrep("-", 48)))

for (yr in SEASONS) {
  sub <- v2_valid[v2_valid$season == yr, ]
  if (nrow(sub) < 50) next

  # v1+PF2025
  s1 <- vapply(seq_len(nrow(sub)), function(i) {
    vals <- c(sub$sp_skillz_index[i], sub$team_rater_inv[i], sub$park_factor_inv[i])
    wts <- c(0.6, 0.3, 0.1)
    ok <- !is.na(vals) & wts > 0
    if (!any(ok)) return(NA_real_)
    sum(vals[ok] * wts[ok]) / sum(wts[ok])
  }, numeric(1))

  # v2+PF2026
  s2 <- vapply(seq_len(nrow(sub)), function(i) {
    vals <- c(sub$v2_sp_skillz_index[i], sub$team_rater_inv[i], sub$park_factor_inv_2026[i])
    wts <- c(0.6, 0.3, 0.1)
    ok <- !is.na(vals) & wts > 0
    if (!any(ok)) return(NA_real_)
    sum(vals[ok] * wts[ok]) / sum(wts[ok])
  }, numeric(1))

  ok <- !is.na(s1) & !is.na(s2) & !is.na(sub$good_start_score)
  if (sum(ok) < 30) next

  rho1 <- cor(s1[ok], sub$good_start_score[ok], method = "spearman")
  rho2 <- cor(s2[ok], sub$good_start_score[ok], method = "spearman")

  cat(sprintf("  %-8d  %8.4f  %8.4f  %+8.4f  %7d\n",
              yr, rho1, rho2, rho2 - rho1, sum(ok)))
}

cat("\nDone.\n")
