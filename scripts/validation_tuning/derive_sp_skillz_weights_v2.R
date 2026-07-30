#!/usr/bin/env Rscript
# ===========================================================================
# SP Skillz Weight Derivation v2 — Final Empirical Model
# ===========================================================================
# Metrics (SEVEN — the header used to list six and predate arsenal_breadth;
#   corrected 2026-07-30): k_minus_bb_pct, whiff_pct, stuff_plus,
#   pitching_plus, ball_pct, high_gb_flag, arsenal_breadth
# Targets: next_k_pct (1/3), next_whip (1/3), next_siera (2/9), next_era (1/9)
# Method: Ridge regression with LOYO-CV, universal weights, 2-pass starter-pool z-scoring
# Data: 2021-2025, qualified SPs (start_share >= 2/3, TBF >= 100)
# Stuff+/Pitching+ use fixed-scale (x-100)/10 per live model convention
#
# ── THIS IS THE SCRIPT THAT PRODUCES THE LIVE WEIGHTS ──────────────────────
#   Not exploratory. Whatever this emits is what the shipped SP Skillz model
#   uses, so re-running it CHANGES THE PRODUCT. set.seed(20260715) is load-
#   bearing: cv.glmnet picks lambda on random CV folds, so without the fixed
#   seed the weights move run to run. Do not remove or change that seed
#   casually — it is why the published weights are reproducible.
#
# ── RECORDED WEIGHTS ──────────────────────────────────────────────────────
#   Canonical seeded run, GROUPED pitch taxonomy:
#     kbb 2.0000 | whiff 1.3027 | stuff+ 0.7118 | pitching+ 1.1660
#     ball 0.0778 | gb 0.3660 | arsenal 0.6133
#   Adding arsenal_breadth lifted next-WHIP CV R^2 from 0.248 to 0.258, with
#   K% flat — which is the point: breadth is a WHIP / run-shape signal, not a
#   strikeout signal. That asymmetry is the mechanism check, not a side note.
#
#   SHIPPED + DEPLOYED 2026-07-18 under the SWEEPER-AWARE GRANULAR taxonomy
#   this file now implements (sweeper and slurve count as pitches distinct
#   from slider). Under granular, the breadth weight settled at 0.5153
#   (down from the 0.6133 above). CV R^2 came out flat versus grouped:
#     K 0.535 | WHIP 0.257 | SIERA 0.322 | ERA 0.153
#   ⚠ Only the BREADTH weight is separately recorded for the granular run.
#     The other six above are from the grouped run — do not assume they are
#     byte-identical under granular. Re-run if you need all seven exactly.
#
#   Related history: an earlier "counting sub-pitches LOSES, keep the parent
#   taxonomy" finding was later OVERTURNED by the sweeper-aware work. The
#   granular taxonomy is current.
# ===========================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(glmnet)
  library(jsonlite)
})

# Reproducible weights: cv.glmnet uses random CV folds for lambda selection.
set.seed(20260715)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

SEASONS <- 2021:2025
CACHE_DIR <- file.path("data", "raw", "sp_skillz_validation_cache")
ARS_DIR   <- file.path("data", "raw", "savant_arsenals")   # v2.1 arsenal breadth
OUTPUT_DIR <- file.path("data", "processed", "sp_skillz_v2")
dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

# Skill metrics. v2 = the 6 that passed all empirical tests; v2.1 adds
# arsenal_breadth (count of pitch families used >= 10%, Savant SoT) — a
# WHIP/run-shape signal validated in the arsenal research (2-pitch cliff).
SKILL_METRICS <- c("k_minus_bb_pct", "whiff_pct", "stuff_plus", "pitching_plus",
                   "ball_pct", "high_gb_flag", "arsenal_breadth")

# Metrics that use fixed-scale (x-100)/10 instead of z-scoring
PLUS_METRICS <- c("stuff_plus", "pitching_plus")

# Target blend weights (must sum to 1)
TARGET_BLEND <- c(
  next_k_pct = 1/3,
  next_whip = 1/3,
  next_siera = 2/9,
  next_era = 1/9
)
stopifnot(abs(sum(TARGET_BLEND) - 1) < 1e-10)

# Model parameters
MIN_TBF_FULL <- 100
MIN_IP_NEXT_YEAR <- 40
START_SHARE_MIN <- 2/3
STARTER_POOL_SIZE <- 150  # top N SPs for 2-pass z-scoring

# Stabilization points (TBF or pitches depending on metric)
STABILIZATION_POINTS <- c(
  k_minus_bb_pct = 120,  # TBF-based
  whiff_pct = 100,       # pitches-based (similar to SwStr%)
  stuff_plus = 100,      # pitches-based (model, stabilizes fast)
  pitching_plus = 300,   # pitches-based (broader model, slower)
  ball_pct = 150,        # pitches-based
  high_gb_flag = 150,    # BIP-based (~150 TBF equivalent)
  arsenal_breadth = 150  # pitches-based; light touch (mix converges fast — Phase 2)
)

# Sample source for reliability calculation
SAMPLE_SOURCE <- c(
  k_minus_bb_pct = "tbf",
  whiff_pct = "pitches",
  stuff_plus = "pitches",
  pitching_plus = "pitches",
  ball_pct = "pitches",
  high_gb_flag = "tbf",
  arsenal_breadth = "pitches"
)

cat("=" |> rep(70) |> paste(collapse = ""), "\n")
cat("SP SKILLZ v2 — FINAL WEIGHT DERIVATION\n")
cat("=" |> rep(70) |> paste(collapse = ""), "\n")
cat(sprintf("Metrics: %s\n", paste(SKILL_METRICS, collapse = ", ")))
cat(sprintf("Target blend: K%%=%.1f%%, WHIP=%.1f%%, SIERA=%.1f%%, ERA=%.1f%%\n",
            100*TARGET_BLEND["next_k_pct"], 100*TARGET_BLEND["next_whip"],
            100*TARGET_BLEND["next_siera"], 100*TARGET_BLEND["next_era"]))
cat(sprintf("Seasons: %s\n", paste(SEASONS, collapse = ", ")))
cat("=" |> rep(70) |> paste(collapse = ""), "\n\n")

# ---------------------------------------------------------------------------
# DATA LOADING
# ---------------------------------------------------------------------------

nc <- function(x) suppressWarnings(as.numeric(x))
ipb <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  out <- rep(NA_real_, length(x))
  keep <- !is.na(x)
  if (!any(keep)) return(out)
  whole <- floor(x[keep])
  frac <- round((x[keep] - whole) * 10)
  out[keep] <- whole + ifelse(frac == 1, 1/3, ifelse(frac == 2, 2/3, x[keep] - whole))
  out
}

cat("Loading data...\n")
fg_full <- bind_rows(lapply(SEASONS, function(s) {
  f <- file.path(CACHE_DIR, sprintf("fg_full_%d_v2.json", s))
  raw <- fromJSON(f, flatten = TRUE)$data
  get_col <- function(candidates) {
    for (c in candidates) { if (c %in% names(raw)) return(raw[[c]]) }
    rep(NA, nrow(raw))
  }
  data.frame(
    player_id = as.integer(get_col(c("xMLBAMID", "MLBAMID", "playerid"))),
    player_name = as.character(get_col(c("PlayerName", "Name"))),
    season = s,
    age = nc(get_col(c("Age"))),
    team = as.character(get_col(c("TeamNameAbb", "Team"))),
    ip = ipb(get_col(c("IP"))),
    gs = nc(get_col(c("GS"))),
    g = nc(get_col(c("G"))),
    tbf = nc(get_col(c("TBF"))),
    pitches = nc(get_col(c("Pitches"))),
    start_ip = ipb(get_col(c("Start-IP"))),
    relief_ip = ipb(get_col(c("Relief-IP"))),
    era = nc(get_col(c("ERA"))),
    whip = nc(get_col(c("WHIP"))),
    k_pct = nc(get_col(c("K%"))),
    bb_pct = nc(get_col(c("BB%"))),
    siera = nc(get_col(c("SIERA"))),
    xfip = nc(get_col(c("xFIP"))),
    k_minus_bb_pct = nc(get_col(c("K-BB%"))),
    balls = nc(get_col(c("Balls"))),
    stuff_plus_raw = nc(get_col(c("sp_stuff"))),
    pitching_plus_raw = nc(get_col(c("sp_pitching"))),
    gb_pct = nc(get_col(c("GB%"))),
    stringsAsFactors = FALSE
  )
}))

# Savant whiff
savant_full <- bind_rows(lapply(SEASONS, function(s) {
  f <- file.path(CACHE_DIR, sprintf("savant_whiff_full_%d.csv", s))
  d <- read_csv(f, show_col_types = FALSE)
  data.frame(
    player_id = as.integer(d$player_id),
    season = s,
    whiff_pct = 100 * as.numeric(d$whiffs) / as.numeric(d$swings),
    stringsAsFactors = FALSE
  )
}))
savant_full <- savant_full[!duplicated(savant_full[, c("player_id", "season")]), ]
fg_full <- merge(fg_full, savant_full, by = c("player_id", "season"), all.x = TRUE)

# Derived columns
fg_full$ball_pct <- ifelse(!is.na(fg_full$pitches) & fg_full$pitches > 0,
                           100 * fg_full$balls / fg_full$pitches, NA_real_)
fg_full$start_ip[is.na(fg_full$start_ip)] <- 0
fg_full$relief_ip[is.na(fg_full$relief_ip)] <- 0
fg_full$start_share <- ifelse(
  (fg_full$start_ip + fg_full$relief_ip) > 0,
  fg_full$start_ip / (fg_full$start_ip + fg_full$relief_ip), NA_real_)
fg_full$high_gb_flag <- as.integer(!is.na(fg_full$gb_pct) & fg_full$gb_pct >= 0.55)

# Fixed-scale for Stuff+/Pitching+ (per live model: (x-100)/10)
fg_full$stuff_plus <- (fg_full$stuff_plus_raw - 100) / 10
fg_full$pitching_plus <- (fg_full$pitching_plus_raw - 100) / 10

fg_full$ip_per_gs <- ifelse(!is.na(fg_full$gs) & fg_full$gs > 0, fg_full$ip / fg_full$gs, NA_real_)

# Arsenal breadth (Savant SoT): count of DISTINCT pitches used >= 10% of the time.
# Sweeper (st) and slurve (sv) are counted as their own pitches, separate from the
# slider (sl) — they are distinct pitches by movement/velo/platoon (FG sweeper
# research), and Savant's leaderboard reports them separately. Leaderboard folds
# KC->cu, FO->fs, CS->cu (no separate n_ columns), so those stay in cu/fs.
# Read WITHOUT fileEncoding (BOM trap); id col "pitcher" = MLBAMID.
ARS_NATIVE <- c("ff","si","fc","sl","ch","cu","fs","kn","st","sv")
arsenal <- bind_rows(lapply(SEASONS, function(s) {
  f <- file.path(ARS_DIR, sprintf("savant_arsenals_%d.csv", s))
  if (!file.exists(f)) return(NULL)
  d <- read.csv(f, check.names = FALSE)
  names(d)[1] <- "name"
  u <- vapply(ARS_NATIVE, function(t) {
    v <- suppressWarnings(as.numeric(d[[paste0("n_", t)]])) / 100
    ifelse(is.na(v), 0, v)
  }, numeric(nrow(d)))
  if (is.null(dim(u))) u <- matrix(u, nrow = nrow(d), dimnames = list(NULL, ARS_NATIVE))
  G <- cbind(ff = u[, "ff"], si = u[, "si"], fc = u[, "fc"],
             sl = u[, "sl"], st = u[, "st"], sv = u[, "sv"],
             cu = u[, "cu"], ch = u[, "ch"], fs = u[, "fs"], kn = u[, "kn"])
  data.frame(player_id = as.integer(d$pitcher), season = s,
             arsenal_breadth = rowSums(G >= 0.10))
}))
fg_full <- merge(fg_full, arsenal, by = c("player_id", "season"), all.x = TRUE)
cat(sprintf("  Arsenal breadth matched: %d of %d FG rows have Savant arsenal data\n",
            sum(!is.na(fg_full$arsenal_breadth)), nrow(fg_full)))

# Filter to qualified SPs
sp_full <- fg_full %>%
  filter(!is.na(start_share), start_share >= START_SHARE_MIN,
         !is.na(tbf), tbf >= MIN_TBF_FULL)

cat(sprintf("  Qualified SP-seasons: %d\n", nrow(sp_full)))

# ---------------------------------------------------------------------------
# BUILD YOY PAIRS
# ---------------------------------------------------------------------------

cat("Building YoY pairs...\n")
base <- sp_full %>%
  select(player_id, season, all_of(SKILL_METRICS), start_ip, tbf, pitches)
target <- sp_full %>%
  select(player_id, season, k_pct, siera, whip, era, ip) %>%
  rename(next_k_pct = k_pct, next_siera = siera, next_whip = whip,
         next_era = era, next_ip = ip) %>%
  mutate(season = season - 1L)

pairs <- inner_join(base, target, by = c("player_id", "season")) %>%
  filter(!is.na(next_ip), next_ip >= MIN_IP_NEXT_YEAR)

cat(sprintf("  YoY pairs: %d (across %d season transitions)\n\n",
            nrow(pairs), n_distinct(pairs$season)))

# ---------------------------------------------------------------------------
# RIDGE REGRESSION — DERIVE WEIGHTS PER TARGET
# ---------------------------------------------------------------------------

cat("Deriving ridge weights per target (LOYO-CV)...\n\n")

ridge_fit_and_cv <- function(pairs_df, metric_cols, target_col) {
  complete <- pairs_df %>%
    select(season, all_of(metric_cols), all_of(target_col)) %>%
    filter(complete.cases(.))
  
  if (nrow(complete) < 30) return(NULL)
  seasons <- sort(unique(complete$season))
  if (length(seasons) < 3) return(NULL)
  
  # LOYO-CV for R²
  cv_r2s <- numeric(length(seasons))
  for (i in seq_along(seasons)) {
    train <- complete[complete$season != seasons[i], ]
    test <- complete[complete$season == seasons[i], ]
    if (nrow(train) < 20 || nrow(test) < 10) { cv_r2s[i] <- NA_real_; next }
    
    X_train <- as.matrix(train[, metric_cols, drop = FALSE])
    y_train <- train[[target_col]]
    X_test <- as.matrix(test[, metric_cols, drop = FALSE])
    y_test <- test[[target_col]]
    
    means <- colMeans(X_train); sds <- apply(X_train, 2, sd); sds[sds == 0] <- 1
    X_train_s <- scale(X_train, center = means, scale = sds)
    X_test_s <- scale(X_test, center = means, scale = sds)
    
    fit <- tryCatch(cv.glmnet(X_train_s, y_train, alpha = 0, nfolds = 5), error = function(e) NULL)
    if (is.null(fit)) { cv_r2s[i] <- NA_real_; next }
    
    preds <- as.numeric(predict(fit, X_test_s, s = "lambda.min"))
    ss_res <- sum((y_test - preds)^2)
    ss_tot <- sum((y_test - mean(y_test))^2)
    cv_r2s[i] <- ifelse(ss_tot > 0, 1 - ss_res / ss_tot, NA_real_)
  }
  
  valid_r2 <- cv_r2s[is.finite(cv_r2s)]
  cv_r2 <- if (length(valid_r2) > 0) mean(valid_r2) else NA_real_
  
  # Full-data fit for final coefficients
  X_all <- as.matrix(complete[, metric_cols, drop = FALSE])
  y_all <- complete[[target_col]]
  means_all <- colMeans(X_all); sds_all <- apply(X_all, 2, sd); sds_all[sds_all == 0] <- 1
  X_all_s <- scale(X_all, center = means_all, scale = sds_all)
  
  fit_full <- cv.glmnet(X_all_s, y_all, alpha = 0, nfolds = 10)
  coefs <- as.numeric(coef(fit_full, s = "lambda.min"))[-1]
  names(coefs) <- metric_cols
  
  list(
    coefficients = coefs,
    cv_r2 = cv_r2,
    n = nrow(complete),
    lambda = fit_full$lambda.min
  )
}

# Fit per target
target_results <- list()
for (target in names(TARGET_BLEND)) {
  result <- ridge_fit_and_cv(pairs, SKILL_METRICS, target)
  if (!is.null(result)) {
    target_results[[target]] <- result
    cat(sprintf("  %s: CV R²=%.4f (n=%d)\n", target, result$cv_r2, result$n))
    cat(sprintf("    Coefficients: %s\n",
                paste(sprintf("%s=%.4f", names(result$coefficients), result$coefficients), collapse = ", ")))
  }
}

# ---------------------------------------------------------------------------
# BLEND WEIGHTS ACROSS TARGETS
# ---------------------------------------------------------------------------

cat("\nBlending across targets...\n")

# For each metric, compute blended weight as weighted average of per-target coefficients
# Direction: we want positive = better pitcher, so flip sign for lower-is-better targets
TARGET_SIGN <- c(next_k_pct = 1, next_siera = -1, next_whip = -1, next_era = -1)

blended_weights <- numeric(length(SKILL_METRICS))
names(blended_weights) <- SKILL_METRICS

for (m in SKILL_METRICS) {
  weighted_coef <- 0
  for (target in names(TARGET_BLEND)) {
    if (!target %in% names(target_results)) next
    # Sign-flip: make coefficient "quality-aligned" (positive = good pitcher)
    aligned_coef <- target_results[[target]]$coefficients[[m]] * TARGET_SIGN[[target]]
    weighted_coef <- weighted_coef + aligned_coef * TARGET_BLEND[[target]]
  }
  blended_weights[[m]] <- weighted_coef
}

# Scale so max absolute weight = 2 (matching live model convention)
max_abs <- max(abs(blended_weights))
if (max_abs > 0) {
  blended_weights <- (blended_weights / max_abs) * 2
}

cat("\n  FINAL BLENDED WEIGHTS (scaled to max=2):\n")
for (m in SKILL_METRICS) {
  cat(sprintf("    %s: %.3f\n", m, blended_weights[m]))
}

# ---------------------------------------------------------------------------
# 2-PASS SCORING: Apply to each season
# ---------------------------------------------------------------------------

cat("\n\nApplying 2-pass scoring to each season...\n")

compute_reliability <- function(sample_size, stab_point) {
  ifelse(is.na(sample_size) | sample_size <= 0, 0,
         sample_size / (sample_size + stab_point))
}

score_season <- function(season_data, weights, starter_pool_n = STARTER_POOL_SIZE) {
  df <- season_data
  n <- nrow(df)
  
  # --- Pass 1: Global z-scores, identify starter pool ---
  z_mat <- matrix(NA_real_, nrow = n, ncol = length(SKILL_METRICS))
  colnames(z_mat) <- SKILL_METRICS
  
  for (m in SKILL_METRICS) {
    vals <- df[[m]]
    if (m %in% PLUS_METRICS) {
      # Already in (x-100)/10 scale — use as-is (no re-z-scoring)
      z_mat[, m] <- vals
    } else {
      mu <- mean(vals, na.rm = TRUE)
      s <- sd(vals, na.rm = TRUE)
      if (!is.na(s) && s > 0) {
        z_mat[, m] <- (vals - mu) / s
      } else {
        z_mat[, m] <- 0
      }
    }
  }
  
  # Compute initial scores (no reliability, just weighted z-sum)
  initial_scores <- numeric(n)
  for (i in seq_len(n)) {
    z_row <- z_mat[i, ]
    keep <- !is.na(z_row) & weights != 0
    if (any(keep)) {
      initial_scores[i] <- sum(z_row[keep] * weights[keep]) / sum(abs(weights[keep]))
    } else {
      initial_scores[i] <- NA_real_
    }
  }
  
  # Identify starter pool (top N by initial score)
  pool_n <- min(starter_pool_n, sum(!is.na(initial_scores)))
  pool_idx <- order(initial_scores, decreasing = TRUE, na.last = NA)[seq_len(pool_n)]
  
  # --- Pass 2: Re-z relative to starter pool ---
  z_pool <- matrix(NA_real_, nrow = n, ncol = length(SKILL_METRICS))
  colnames(z_pool) <- SKILL_METRICS
  
  for (m in SKILL_METRICS) {
    vals <- df[[m]]
    if (m %in% PLUS_METRICS) {
      z_pool[, m] <- vals  # fixed scale, no re-centering
    } else {
      pool_vals <- vals[pool_idx]
      mu_pool <- mean(pool_vals, na.rm = TRUE)
      s_pool <- sd(pool_vals, na.rm = TRUE)
      if (!is.na(s_pool) && s_pool > 0) {
        z_pool[, m] <- (vals - mu_pool) / s_pool
      } else {
        z_pool[, m] <- 0
      }
    }
  }
  
  # Compute reliability per metric per player
  rel_mat <- matrix(1, nrow = n, ncol = length(SKILL_METRICS))
  colnames(rel_mat) <- SKILL_METRICS
  for (m in SKILL_METRICS) {
    source_col <- SAMPLE_SOURCE[[m]]
    samples <- df[[source_col]]
    rel_mat[, m] <- compute_reliability(samples, STABILIZATION_POINTS[[m]])
  }
  
  # Effective weights = base weights * reliability
  # Final score = weighted z-sum with effective weights, normalized by base weight sum
  scores <- numeric(n)
  scores_stabilized <- numeric(n)
  target_denom <- sum(abs(weights[weights != 0]))
  
  for (i in seq_len(n)) {
    z_row <- z_pool[i, ]
    rel_row <- rel_mat[i, ]
    keep <- !is.na(z_row) & weights != 0
    
    if (any(keep)) {
      # Raw score (no reliability dampening)
      scores[i] <- sum(z_row[keep] * weights[keep]) / target_denom
      # Stabilized score (reliability-dampened)
      eff_weights <- weights * rel_row
      scores_stabilized[i] <- sum(z_row[keep] * eff_weights[keep]) / target_denom
    } else {
      scores[i] <- NA_real_
      scores_stabilized[i] <- NA_real_
    }
  }
  
  df$sp_skillz_score <- scores
  df$sp_skillz_score_stabilized <- scores_stabilized
  df$sp_skillz_rank <- rank(-scores, ties.method = "min", na.last = "keep")
  df$sp_skillz_rank_stabilized <- rank(-scores_stabilized, ties.method = "min", na.last = "keep")
  df$sp_skillz_reliability <- rowMeans(rel_mat, na.rm = TRUE)
  df$sp_skillz_starter_pool <- FALSE
  df$sp_skillz_starter_pool[pool_idx] <- TRUE
  
  # Add z-scores and reliability columns for transparency
  for (m in SKILL_METRICS) {
    df[[paste0(m, "_z")]] <- z_pool[, m]
    df[[paste0(m, "_rel")]] <- rel_mat[, m]
  }
  
  df
}

# Score each season
all_scored <- list()
for (s in SEASONS) {
  season_sp <- sp_full %>% filter(season == s)
  if (nrow(season_sp) < 30) next
  scored <- score_season(season_sp, blended_weights)
  all_scored[[as.character(s)]] <- scored
  cat(sprintf("  %d: %d SPs scored (pool=%d)\n", s, nrow(scored),
              sum(scored$sp_skillz_starter_pool)))
}

# Combine all seasons
full_table <- bind_rows(all_scored)
cat(sprintf("\n  Total scored: %d player-seasons\n", nrow(full_table)))

# ---------------------------------------------------------------------------
# CACHE FULL-SEASON TABLE
# ---------------------------------------------------------------------------

# Select display columns
display_cols <- c(
  "player_id", "player_name", "team", "season", "age",
  "ip", "gs", "tbf", "pitches", "start_ip", "ip_per_gs",
  "k_minus_bb_pct", "whiff_pct", "stuff_plus_raw", "pitching_plus_raw",
  "ball_pct", "high_gb_flag", "gb_pct", "arsenal_breadth",
  "era", "whip", "k_pct", "bb_pct", "siera", "xfip",
  "sp_skillz_score", "sp_skillz_score_stabilized",
  "sp_skillz_rank", "sp_skillz_rank_stabilized",
  "sp_skillz_reliability", "sp_skillz_starter_pool",
  paste0(SKILL_METRICS, "_z"), paste0(SKILL_METRICS, "_rel")
)
display_cols <- intersect(display_cols, names(full_table))

output_full <- full_table %>%
  select(all_of(display_cols)) %>%
  arrange(season, sp_skillz_rank_stabilized)

write_csv(output_full, file.path(OUTPUT_DIR, "sp_skillz_v2_full_seasons_2021_2025.csv"))
cat(sprintf("\n  Cached: %s\n", file.path(OUTPUT_DIR, "sp_skillz_v2_full_seasons_2021_2025.csv")))

# ---------------------------------------------------------------------------
# LOW-IP VERSION: March/April/May only (early-season)
# ---------------------------------------------------------------------------

cat("\nBuilding low-IP (March-May) version...\n")

# Load 1H data and filter to early season only
# We'll use the 1H cache which covers opening day through ASG
# But we want March-May only. Since FG doesn't have month=3-5 easily,
# we'll simulate by filtering to pitchers with LOW start_ip in their full-season data
# Actually, better approach: use the 1H data (which is ~half season) and apply
# a stricter TBF filter to represent early-season samples

# For a proper "early season" view, let's just take the full-season data
# but lower the TBF threshold and let reliability weighting do its job
# This matches how the live app works: same model, just less data = more dampening

LOW_IP_MIN_TBF <- 30  # ~5-6 starts worth
LOW_IP_SEASONS <- SEASONS

fg_1h <- bind_rows(lapply(LOW_IP_SEASONS, function(s) {
  f <- file.path(CACHE_DIR, sprintf("fg_1h_%d_v2.json", s))
  if (!file.exists(f)) return(NULL)
  raw <- fromJSON(f, flatten = TRUE)$data
  get_col <- function(candidates) {
    for (c in candidates) { if (c %in% names(raw)) return(raw[[c]]) }
    rep(NA, nrow(raw))
  }
  data.frame(
    player_id = as.integer(get_col(c("xMLBAMID", "MLBAMID", "playerid"))),
    player_name = as.character(get_col(c("PlayerName", "Name"))),
    season = s,
    age = nc(get_col(c("Age"))),
    team = as.character(get_col(c("TeamNameAbb", "Team"))),
    ip = ipb(get_col(c("IP"))),
    gs = nc(get_col(c("GS"))),
    g = nc(get_col(c("G"))),
    tbf = nc(get_col(c("TBF"))),
    pitches = nc(get_col(c("Pitches"))),
    start_ip = ipb(get_col(c("Start-IP"))),
    relief_ip = ipb(get_col(c("Relief-IP"))),
    era = nc(get_col(c("ERA"))),
    whip = nc(get_col(c("WHIP"))),
    k_pct = nc(get_col(c("K%"))),
    bb_pct = nc(get_col(c("BB%"))),
    siera = nc(get_col(c("SIERA"))),
    xfip = nc(get_col(c("xFIP"))),
    k_minus_bb_pct = nc(get_col(c("K-BB%"))),
    balls = nc(get_col(c("Balls"))),
    stuff_plus_raw = nc(get_col(c("sp_stuff"))),
    pitching_plus_raw = nc(get_col(c("sp_pitching"))),
    gb_pct = nc(get_col(c("GB%"))),
    stringsAsFactors = FALSE
  )
}))

# Merge savant 1H whiff
savant_1h <- bind_rows(lapply(LOW_IP_SEASONS, function(s) {
  f <- file.path(CACHE_DIR, sprintf("savant_whiff_1h_%d.csv", s))
  if (!file.exists(f)) return(NULL)
  d <- read_csv(f, show_col_types = FALSE)
  data.frame(
    player_id = as.integer(d$player_id),
    season = s,
    whiff_pct = 100 * as.numeric(d$whiffs) / as.numeric(d$swings),
    stringsAsFactors = FALSE
  )
}))
savant_1h <- savant_1h[!duplicated(savant_1h[, c("player_id", "season")]), ]
fg_1h <- merge(fg_1h, savant_1h, by = c("player_id", "season"), all.x = TRUE)

# Derived columns
fg_1h$ball_pct <- ifelse(!is.na(fg_1h$pitches) & fg_1h$pitches > 0,
                         100 * fg_1h$balls / fg_1h$pitches, NA_real_)
fg_1h$start_ip[is.na(fg_1h$start_ip)] <- 0
fg_1h$relief_ip[is.na(fg_1h$relief_ip)] <- 0
fg_1h$start_share <- ifelse(
  (fg_1h$start_ip + fg_1h$relief_ip) > 0,
  fg_1h$start_ip / (fg_1h$start_ip + fg_1h$relief_ip), NA_real_)
fg_1h$high_gb_flag <- as.integer(!is.na(fg_1h$gb_pct) & fg_1h$gb_pct >= 0.55)
fg_1h$stuff_plus <- (fg_1h$stuff_plus_raw - 100) / 10
fg_1h$pitching_plus <- (fg_1h$pitching_plus_raw - 100) / 10
fg_1h$ip_per_gs <- ifelse(!is.na(fg_1h$gs) & fg_1h$gs > 0, fg_1h$ip / fg_1h$gs, NA_real_)
# Full-season arsenal breadth (stable within a season; the live app likewise
# reads season-to-date arsenal early in the year, so this mirrors production).
fg_1h <- merge(fg_1h, arsenal, by = c("player_id", "season"), all.x = TRUE)

# Filter: starters with at least LOW_IP_MIN_TBF
sp_1h <- fg_1h %>%
  filter(!is.na(start_share), start_share >= START_SHARE_MIN,
         !is.na(tbf), tbf >= LOW_IP_MIN_TBF)

cat(sprintf("  1H SP-seasons (TBF >= %d): %d\n", LOW_IP_MIN_TBF, nrow(sp_1h)))

# Score with same weights — reliability dampening handles the rest
all_scored_1h <- list()
for (s in LOW_IP_SEASONS) {
  season_sp <- sp_1h %>% filter(season == s)
  if (nrow(season_sp) < 30) next
  scored <- score_season(season_sp, blended_weights)
  all_scored_1h[[as.character(s)]] <- scored
  cat(sprintf("  %d: %d SPs scored (pool=%d)\n", s, nrow(scored),
              sum(scored$sp_skillz_starter_pool)))
}

full_table_1h <- bind_rows(all_scored_1h)

display_cols_1h <- intersect(display_cols, names(full_table_1h))
output_1h <- full_table_1h %>%
  select(all_of(display_cols_1h)) %>%
  arrange(season, sp_skillz_rank_stabilized)

write_csv(output_1h, file.path(OUTPUT_DIR, "sp_skillz_v2_first_half_2021_2025.csv"))
cat(sprintf("\n  Cached: %s\n", file.path(OUTPUT_DIR, "sp_skillz_v2_first_half_2021_2025.csv")))

# ---------------------------------------------------------------------------
# SAVE WEIGHTS & METADATA
# ---------------------------------------------------------------------------

weights_meta <- data.frame(
  metric = SKILL_METRICS,
  weight = round(blended_weights, 4),
  stabilization_point = STABILIZATION_POINTS[SKILL_METRICS],
  sample_source = SAMPLE_SOURCE[SKILL_METRICS],
  stringsAsFactors = FALSE
)
write_csv(weights_meta, file.path(OUTPUT_DIR, "sp_skillz_v2_weights.csv"))

target_meta <- data.frame(
  target = names(TARGET_BLEND),
  blend_weight = as.numeric(TARGET_BLEND),
  cv_r2 = sapply(names(TARGET_BLEND), function(t) {
    if (t %in% names(target_results)) target_results[[t]]$cv_r2 else NA_real_
  }),
  stringsAsFactors = FALSE
)
write_csv(target_meta, file.path(OUTPUT_DIR, "sp_skillz_v2_target_blend.csv"))

cat("\n\n")
cat("=" |> rep(70) |> paste(collapse = ""), "\n")
cat("DONE — ALL OUTPUTS\n")
cat("=" |> rep(70) |> paste(collapse = ""), "\n")
cat(sprintf("  %s/sp_skillz_v2_full_seasons_2021_2025.csv\n", OUTPUT_DIR))
cat(sprintf("  %s/sp_skillz_v2_first_half_2021_2025.csv\n", OUTPUT_DIR))
cat(sprintf("  %s/sp_skillz_v2_weights.csv\n", OUTPUT_DIR))
cat(sprintf("  %s/sp_skillz_v2_target_blend.csv\n", OUTPUT_DIR))
cat("\n  FINAL WEIGHTS:\n")
for (m in SKILL_METRICS) {
  cat(sprintf("    %-18s: %7.3f (stab=%d)\n", m, blended_weights[m], STABILIZATION_POINTS[m]))
}
cat("\n")
