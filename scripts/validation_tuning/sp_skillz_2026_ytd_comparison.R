#!/usr/bin/env Rscript
# ===========================================================================
# SP Skillz 2026 YTD — Score with BOTH v1 and v2, compare rankings
# ===========================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(jsonlite)
})

base_dir <- "/Users/ckaufman/Documents/New project"

# ── Helper functions ─────────────────────────────────────────────────
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

# ══════════════════════════════════════════════════════════════════════
# STEP 1: Fetch 2026 YTD data from FanGraphs + Savant
# ══════════════════════════════════════════════════════════════════════

cat("======================================================================\n")
cat("FETCHING 2026 YTD DATA\n")
cat("======================================================================\n\n")

# FG full-season (YTD) 2026
fg_url <- paste0(
  "https://www.fangraphs.com/api/leaders/major-league/data",
  "?pos=all&stats=pit&lg=all&qual=0&season=2026&season1=2026",
  "&month=0&hand=&team=&pageitems=2000&pagenum=1&ind=0",
  "&rost=0&players=&type=c,4,5,6,7,8,9,11,12,13,14,15,16,17,18,24,36,37,38,40,41,42,43,44,45,120,121,217,218,328,329,330,331"
)

fg_raw <- tryCatch(fromJSON(fg_url, flatten = TRUE)$data, error = function(e) {
  cat("  ERROR fetching FG data:", conditionMessage(e), "\n")
  NULL
})

if (is.null(fg_raw)) stop("Cannot fetch FG 2026 data.")
cat(sprintf("  FG 2026 YTD: %d rows\n", nrow(fg_raw)))

# Parse FG data
get_col <- function(candidates, data) {
  for (c in candidates) { if (c %in% names(data)) return(data[[c]]) }
  rep(NA, nrow(data))
}

fg <- data.frame(
  player_id = as.integer(get_col(c("xMLBAMID", "MLBAMID", "playerid"), fg_raw)),
  player_name = as.character(get_col(c("PlayerName", "Name"), fg_raw)),
  team = as.character(get_col(c("TeamNameAbb", "Team"), fg_raw)),
  ip = ipb(get_col(c("IP"), fg_raw)),
  gs = nc(get_col(c("GS"), fg_raw)),
  g = nc(get_col(c("G"), fg_raw)),
  tbf = nc(get_col(c("TBF"), fg_raw)),
  pitches = nc(get_col(c("Pitches"), fg_raw)),
  start_ip = ipb(get_col(c("Start-IP"), fg_raw)),
  relief_ip = ipb(get_col(c("Relief-IP"), fg_raw)),
  era = nc(get_col(c("ERA"), fg_raw)),
  whip = nc(get_col(c("WHIP"), fg_raw)),
  k_pct = nc(get_col(c("K%"), fg_raw)),
  bb_pct = nc(get_col(c("BB%"), fg_raw)),
  siera = nc(get_col(c("SIERA"), fg_raw)),
  xfip = nc(get_col(c("xFIP"), fg_raw)),
  k_minus_bb_pct = nc(get_col(c("K-BB%"), fg_raw)),
  contact_pct = nc(get_col(c("Contact%"), fg_raw)),
  swstr_pct = nc(get_col(c("SwStr%"), fg_raw)),
  balls = nc(get_col(c("Balls"), fg_raw)),
  stuff_plus_raw = nc(get_col(c("sp_stuff"), fg_raw)),
  pitching_plus_raw = nc(get_col(c("sp_pitching"), fg_raw)),
  gb_pct = nc(get_col(c("GB%"), fg_raw)),
  stringsAsFactors = FALSE
)

# Savant Whiff% for 2026 — use statcast_search CSV endpoint (same as validation script)
cat("  Fetching Savant whiff data...\n")
savant_whiff_url <- sprintf(
  paste0(
    "https://baseballsavant.mlb.com/statcast_search/csv",
    "?hfGT=R%%7C&hfSea=2026%%7C&player_type=pitcher",
    "&game_date_gt=2026-03-01&game_date_lt=2026-11-30",
    "&group_by=name&min_pitches=0&min_results=0&min_pas=0",
    "&sort_col=pitches&sort_order=desc",
    "&chk_stats_whiffs=on&chk_stats_swings=on&chk_stats_pitches=on"
  )
)

savant_raw <- tryCatch({
  tmp <- tempfile(fileext = ".csv")
  download.file(savant_whiff_url, tmp, quiet = TRUE, timeout = 60)
  read_csv(tmp, show_col_types = FALSE)
}, error = function(e) {
  cat("  Savant download failed:", conditionMessage(e), "\n")
  NULL
})

if (!is.null(savant_raw) && "player_id" %in% names(savant_raw) && nrow(savant_raw) > 0) {
  savant <- data.frame(
    player_id = as.integer(savant_raw$player_id),
    whiff_pct = 100 * nc(savant_raw$whiffs) / nc(savant_raw$swings),
    stringsAsFactors = FALSE
  )
  savant <- savant[!is.na(savant$player_id) & !duplicated(savant$player_id), ]
  fg <- merge(fg, savant, by = "player_id", all.x = TRUE)
  cat(sprintf("  Savant whiff data merged: %d matches out of %d SPs\n",
              sum(!is.na(fg$whiff_pct)), nrow(fg)))
} else {
  cat("  WARNING: No Savant whiff data available. v2 will score without Whiff%.\n")
  fg$whiff_pct <- NA_real_
}

# ── Derived columns ──────────────────────────────────────────────────
fg$ball_pct <- ifelse(!is.na(fg$pitches) & fg$pitches > 0, 100 * fg$balls / fg$pitches, NA_real_)
fg$start_ip[is.na(fg$start_ip)] <- 0
fg$relief_ip[is.na(fg$relief_ip)] <- 0
fg$start_share <- ifelse(
  (fg$start_ip + fg$relief_ip) > 0,
  fg$start_ip / (fg$start_ip + fg$relief_ip), NA_real_)
fg$high_gb_flag <- as.integer(!is.na(fg$gb_pct) & fg$gb_pct >= 0.55)
fg$stuff_plus <- (fg$stuff_plus_raw - 100) / 10
fg$pitching_plus <- (fg$pitching_plus_raw - 100) / 10
fg$ip_per_gs <- ifelse(!is.na(fg$gs) & fg$gs > 0, fg$ip / fg$gs, NA_real_)

# Filter to SPs (start_share >= 2/3, at least some TBF)
sp <- fg %>%
  filter(!is.na(start_share), start_share >= 2/3,
         !is.na(tbf), tbf >= 30)  # lower threshold for YTD

cat(sprintf("  Qualified SPs (start_share >= 2/3, TBF >= 30): %d\n\n", nrow(sp)))

# ══════════════════════════════════════════════════════════════════════
# STEP 2: Score with V2 weights
# ══════════════════════════════════════════════════════════════════════

cat("======================================================================\n")
cat("SCORING WITH V2 (UPDATED) MODEL\n")
cat("======================================================================\n\n")

V2_WEIGHTS <- c(
  k_minus_bb_pct = 2.000,
  whiff_pct = 1.313,
  stuff_plus = 0.700,
  pitching_plus = 1.288,
  ball_pct = 0.056,
  high_gb_flag = 0.352
)

V2_METRICS <- names(V2_WEIGHTS)
V2_PLUS_METRICS <- c("stuff_plus", "pitching_plus")

V2_STAB <- c(
  k_minus_bb_pct = 120,
  whiff_pct = 100,
  stuff_plus = 100,
  pitching_plus = 300,
  ball_pct = 150,
  high_gb_flag = 150
)

V2_SAMPLE_SOURCE <- c(
  k_minus_bb_pct = "tbf",
  whiff_pct = "pitches",
  stuff_plus = "pitches",
  pitching_plus = "pitches",
  ball_pct = "pitches",
  high_gb_flag = "tbf"
)

STARTER_POOL_SIZE <- 150

score_v2 <- function(df, weights = V2_WEIGHTS, pool_n = STARTER_POOL_SIZE) {
  n <- nrow(df)
  metrics <- names(weights)

  # Pass 1: Global z-scores → identify starter pool
  z_mat <- matrix(NA_real_, nrow = n, ncol = length(metrics))
  colnames(z_mat) <- metrics

  for (m in metrics) {
    vals <- df[[m]]
    if (m %in% V2_PLUS_METRICS) {
      z_mat[, m] <- vals  # fixed scale
    } else {
      mu <- mean(vals, na.rm = TRUE)
      s <- sd(vals, na.rm = TRUE)
      if (!is.na(s) && s > 0) z_mat[, m] <- (vals - mu) / s else z_mat[, m] <- 0
    }
  }

  # Initial scores (unweighted by reliability)
  init_scores <- numeric(n)
  denom <- sum(abs(weights))
  for (i in seq_len(n)) {
    z_row <- z_mat[i, ]
    keep <- !is.na(z_row)
    if (any(keep)) init_scores[i] <- sum(z_row[keep] * weights[keep]) / denom
    else init_scores[i] <- NA_real_
  }

  pool_n_actual <- min(pool_n, sum(!is.na(init_scores)))
  pool_idx <- order(init_scores, decreasing = TRUE, na.last = NA)[seq_len(pool_n_actual)]

  # Pass 2: Re-z relative to starter pool
  z_pool <- matrix(NA_real_, nrow = n, ncol = length(metrics))
  colnames(z_pool) <- metrics
  for (m in metrics) {
    vals <- df[[m]]
    if (m %in% V2_PLUS_METRICS) {
      z_pool[, m] <- vals
    } else {
      pool_vals <- vals[pool_idx]
      mu_pool <- mean(pool_vals, na.rm = TRUE)
      s_pool <- sd(pool_vals, na.rm = TRUE)
      if (!is.na(s_pool) && s_pool > 0) z_pool[, m] <- (vals - mu_pool) / s_pool
      else z_pool[, m] <- 0
    }
  }

  # Reliability
  rel_mat <- matrix(1, nrow = n, ncol = length(metrics))
  colnames(rel_mat) <- metrics
  for (m in metrics) {
    src <- V2_SAMPLE_SOURCE[[m]]
    samples <- df[[src]]
    rel_mat[, m] <- ifelse(is.na(samples) | samples <= 0, 0, samples / (samples + V2_STAB[[m]]))
  }

  # Final scores
  scores <- numeric(n)
  scores_stab <- numeric(n)
  for (i in seq_len(n)) {
    z_row <- z_pool[i, ]
    rel_row <- rel_mat[i, ]
    keep <- !is.na(z_row)
    if (any(keep)) {
      scores[i] <- sum(z_row[keep] * weights[keep]) / denom
      eff_w <- weights * rel_row
      scores_stab[i] <- sum(z_row[keep] * eff_w[keep]) / denom
    } else {
      scores[i] <- NA_real_
      scores_stab[i] <- NA_real_
    }
  }

  df$v2_score <- scores
  df$v2_score_stab <- scores_stab
  df$v2_rank <- rank(-scores, ties.method = "min", na.last = "keep")
  df$v2_rank_stab <- rank(-scores_stab, ties.method = "min", na.last = "keep")
  df$v2_reliability <- rowMeans(rel_mat, na.rm = TRUE)
  df
}

sp <- score_v2(sp)
cat(sprintf("  V2 scored: %d SPs\n", nrow(sp)))

# ══════════════════════════════════════════════════════════════════════
# STEP 3: Score with V1 (live) weights
# ══════════════════════════════════════════════════════════════════════

cat("\n======================================================================\n")
cat("SCORING WITH V1 (LIVE) MODEL\n")
cat("======================================================================\n\n")

V1_METRICS <- c("tbf", "ip_per_gs", "siera", "xfip", "k_minus_bb_pct",
                "contact_pct", "swstr_pct", "ball_pct", "stuff_plus", "pitching_plus")

V1_DEFAULT_WEIGHTS <- c(
  tbf = 0, ip_per_gs = 0,
  siera = -1, xfip = -1,
  k_minus_bb_pct = 1, contact_pct = -1, swstr_pct = 1,
  ball_pct = -1, stuff_plus = 1.5, pitching_plus = 2
)

V1_LOW_IP_WEIGHTS <- c(
  tbf = 0, ip_per_gs = 0,
  siera = -0.76, xfip = -0.42,
  k_minus_bb_pct = 1.23, contact_pct = -2.0, swstr_pct = 1.76,
  ball_pct = -0.01, stuff_plus = 1.75, pitching_plus = 1.06
)

V1_HIGH_IP_WEIGHTS <- c(
  tbf = 0, ip_per_gs = 0,
  siera = -1.61, xfip = -1.34,
  k_minus_bb_pct = 2.0, contact_pct = -1.73, swstr_pct = 1.84,
  ball_pct = -0.58, stuff_plus = 1.60, pitching_plus = 1.63
)

V1_STAB <- c(
  tbf = 1, ip_per_gs = 1, siera = 30, xfip = 30,
  k_minus_bb_pct = 120, contact_pct = 300, swstr_pct = 100,
  ball_pct = 150, stuff_plus = 100, pitching_plus = 300
)

V1_PLUS_METRICS <- c("stuff_plus", "pitching_plus")

# v1 uses IP-tier blending
LOW_IP_MAX <- 80
HIGH_IP_MIN <- 120

score_v1 <- function(df) {
  n <- nrow(df)
  metrics <- V1_METRICS

  # Determine weight profile per pitcher
  profiles <- character(n)
  for (i in seq_len(n)) {
    sip <- df$start_ip[i]
    if (is.na(sip) || sip <= LOW_IP_MAX) {
      profiles[i] <- "low_ip"
    } else if (sip >= HIGH_IP_MIN) {
      profiles[i] <- "high_ip"
    } else {
      profiles[i] <- "blended_ip"
    }
  }

  get_weights <- function(profile) {
    if (profile == "low_ip") return(V1_LOW_IP_WEIGHTS)
    if (profile == "high_ip") return(V1_HIGH_IP_WEIGHTS)
    # Blended
    blend_frac <- 0.5  # simplified; live model interpolates based on start_ip
    V1_LOW_IP_WEIGHTS * (1 - blend_frac) + V1_HIGH_IP_WEIGHTS * blend_frac
  }

  # Pass 1: Global z-scores → identify starter pool
  z_mat <- matrix(NA_real_, nrow = n, ncol = length(metrics))
  colnames(z_mat) <- metrics
  for (m in metrics) {
    vals <- df[[m]]
    if (m %in% V1_PLUS_METRICS) {
      z_mat[, m] <- vals  # fixed scale
    } else if (m %in% c("tbf", "ip_per_gs")) {
      z_mat[, m] <- 0  # zero weight
    } else {
      mu <- mean(vals, na.rm = TRUE)
      s <- sd(vals, na.rm = TRUE)
      if (!is.na(s) && s > 0) z_mat[, m] <- (vals - mu) / s else z_mat[, m] <- 0
    }
  }

  # Initial scores with default weights for pool identification
  init_scores <- numeric(n)
  denom <- sum(abs(V1_DEFAULT_WEIGHTS))
  for (i in seq_len(n)) {
    z_row <- z_mat[i, ]
    keep <- !is.na(z_row) & V1_DEFAULT_WEIGHTS != 0
    if (any(keep)) init_scores[i] <- sum(z_row[keep] * V1_DEFAULT_WEIGHTS[keep]) / denom
    else init_scores[i] <- NA_real_
  }

  pool_n <- min(STARTER_POOL_SIZE, sum(!is.na(init_scores)))
  pool_idx <- order(init_scores, decreasing = TRUE, na.last = NA)[seq_len(pool_n)]

  # Pass 2: Re-z relative to starter pool
  z_pool <- matrix(NA_real_, nrow = n, ncol = length(metrics))
  colnames(z_pool) <- metrics
  for (m in metrics) {
    vals <- df[[m]]
    if (m %in% V1_PLUS_METRICS) {
      z_pool[, m] <- vals
    } else if (m %in% c("tbf", "ip_per_gs")) {
      z_pool[, m] <- 0
    } else {
      pool_vals <- vals[pool_idx]
      mu_pool <- mean(pool_vals, na.rm = TRUE)
      s_pool <- sd(pool_vals, na.rm = TRUE)
      if (!is.na(s_pool) && s_pool > 0) z_pool[, m] <- (vals - mu_pool) / s_pool
      else z_pool[, m] <- 0
    }
  }

  # Reliability
  rel_mat <- matrix(1, nrow = n, ncol = length(metrics))
  colnames(rel_mat) <- metrics
  for (m in metrics) {
    # v1 uses TBF for all metrics (simplified)
    samples <- df$tbf
    if (m %in% c("stuff_plus", "pitching_plus", "swstr_pct", "ball_pct")) {
      samples <- df$pitches
    }
    rel_mat[, m] <- ifelse(is.na(samples) | samples <= 0, 0, samples / (samples + V1_STAB[[m]]))
  }

  # Final scores (profile-specific weights)
  scores <- numeric(n)
  scores_stab <- numeric(n)
  for (i in seq_len(n)) {
    w <- get_weights(profiles[i])
    z_row <- z_pool[i, ]
    rel_row <- rel_mat[i, ]
    keep <- !is.na(z_row) & w != 0
    denom_i <- sum(abs(w[w != 0]))
    if (any(keep)) {
      scores[i] <- sum(z_row[keep] * w[keep]) / denom_i
      eff_w <- w * rel_row
      scores_stab[i] <- sum(z_row[keep] * eff_w[keep]) / denom_i
    } else {
      scores[i] <- NA_real_
      scores_stab[i] <- NA_real_
    }
  }

  df$v1_score <- scores
  df$v1_score_stab <- scores_stab
  df$v1_rank <- rank(-scores, ties.method = "min", na.last = "keep")
  df$v1_rank_stab <- rank(-scores_stab, ties.method = "min", na.last = "keep")
  df$v1_reliability <- rowMeans(rel_mat, na.rm = TRUE)
  df$v1_profile <- profiles
  df
}

# Need SwStr% — check if we have it, if not compute from contact%
if (!"swstr_pct" %in% names(sp) || all(is.na(sp$swstr_pct))) {
  # SwStr% might be available from FG as decimal
  cat("  Note: SwStr% check...\n")
}

sp <- score_v1(sp)
cat(sprintf("  V1 scored: %d SPs\n", nrow(sp)))

# ══════════════════════════════════════════════════════════════════════
# STEP 4: Display Results
# ══════════════════════════════════════════════════════════════════════

cat("\n\n")
cat("======================================================================\n")
cat("2026 YTD SP SKILLZ — TOP 30 COMPARISON\n")
cat("======================================================================\n\n")

top <- sp %>%
  select(player_name, team, ip, tbf, v1_rank_stab, v2_rank_stab) %>%
  mutate(delta = v1_rank_stab - v2_rank_stab) %>%
  arrange(v2_rank_stab) %>%
  head(30)

cat("  Sorted by v2 (updated) rank:\n\n")
cat(sprintf("  %-3s  %-24s %-5s %5s %5s  %6s %6s  %5s\n",
            "#", "Player", "Team", "IP", "TBF", "v1Rnk", "v2Rnk", "Delta"))
cat(sprintf("  %-3s  %-24s %-5s %5s %5s  %6s %6s  %5s\n",
            "---", "------------------------", "-----", "-----", "-----", "------", "------", "-----"))

for (i in 1:nrow(top)) {
  r <- top[i, ]
  cat(sprintf("  %-3d  %-24s %-5s %5.1f %5.0f  %5d  %5d  %+4d\n",
              i, substr(r$player_name, 1, 24), r$team, r$ip, r$tbf,
              r$v1_rank_stab, r$v2_rank_stab, r$delta))
}

# ══════════════════════════════════════════════════════════════════════
# STEP 5: Biggest Movers
# ══════════════════════════════════════════════════════════════════════

cat("\n\n")
cat("======================================================================\n")
cat("2026 YTD — BIGGEST MOVERS (v1 vs v2)\n")
cat("======================================================================\n\n")

movers <- sp %>%
  select(player_name, team, ip, tbf, v1_rank_stab, v2_rank_stab,
         k_minus_bb_pct, whiff_pct, stuff_plus_raw, pitching_plus_raw,
         gb_pct, siera, xfip) %>%
  mutate(delta = v1_rank_stab - v2_rank_stab)

# Top 10 Gainers
cat("  TOP 10 GAINERS (ranked higher in v2 'updated' model)\n")
cat("  Positive delta = v2 ranks them higher (better) than v1\n\n")
cat(sprintf("  %-3s  %-24s %-5s %5s %5s  %6s %6s  %5s  %6s %6s %6s %5s\n",
            "#", "Player", "Team", "IP", "TBF", "v1Rnk", "v2Rnk", "Delta",
            "K-BB%", "Whf%", "Stf+", "GB%"))
cat(sprintf("  %-3s  %-24s %-5s %5s %5s  %6s %6s  %5s  %6s %6s %6s %5s\n",
            "---", "------------------------", "-----", "-----", "-----", "------", "------", "-----",
            "------", "------", "------", "-----"))

gainers <- movers %>% arrange(desc(delta)) %>% head(10)
for (i in 1:nrow(gainers)) {
  r <- gainers[i, ]
  cat(sprintf("  %-3d  %-24s %-5s %5.1f %5.0f  %5d  %5d  %+4d  %5.1f%% %5.1f%% %5.0f  %4.1f%%\n",
              i, substr(r$player_name, 1, 24), r$team, r$ip, r$tbf,
              r$v1_rank_stab, r$v2_rank_stab, r$delta,
              r$k_minus_bb_pct * 100, ifelse(is.na(r$whiff_pct), NA, r$whiff_pct),
              r$stuff_plus_raw,
              ifelse(is.na(r$gb_pct), NA, r$gb_pct * 100)))
}

cat("\n\n")

# Top 10 Fallers
cat("  TOP 10 FALLERS (ranked higher in v1 'live' model)\n")
cat("  Negative delta = v1 ranks them higher (better) than v2\n\n")
cat(sprintf("  %-3s  %-24s %-5s %5s %5s  %6s %6s  %5s  %6s %6s %6s %5s\n",
            "#", "Player", "Team", "IP", "TBF", "v1Rnk", "v2Rnk", "Delta",
            "K-BB%", "SIERA", "xFIP", "GB%"))
cat(sprintf("  %-3s  %-24s %-5s %5s %5s  %6s %6s  %5s  %6s %6s %6s %5s\n",
            "---", "------------------------", "-----", "-----", "-----", "------", "------", "-----",
            "------", "------", "------", "-----"))

fallers <- movers %>% arrange(delta) %>% head(10)
for (i in 1:nrow(fallers)) {
  r <- fallers[i, ]
  siera_str <- ifelse(is.na(r$siera), "  NA", sprintf("%5.2f", r$siera))
  xfip_str <- ifelse(is.na(r$xfip), "  NA", sprintf("%5.2f", r$xfip))
  gb_str <- ifelse(is.na(r$gb_pct), " NA", sprintf("%4.1f%%", r$gb_pct * 100))
  kbb_str <- ifelse(is.na(r$k_minus_bb_pct), " NA", sprintf("%4.1f%%", r$k_minus_bb_pct * 100))
  cat(sprintf("  %-3d  %-24s %-5s %5.1f %5.0f  %5d  %5d  %+4d  %s  %s  %s  %s\n",
              i, substr(r$player_name, 1, 24), r$team, r$ip, r$tbf,
              r$v1_rank_stab, r$v2_rank_stab, r$delta,
              kbb_str, siera_str, xfip_str, gb_str))
}

cat("\n\n")
cat("======================================================================\n")
cat("SUMMARY STATS\n")
cat("======================================================================\n\n")

cat(sprintf("  Total SPs scored: %d\n", nrow(sp)))
cat(sprintf("  Rank correlation (v1 vs v2 stabilized): r=%.3f\n",
            cor(sp$v1_rank_stab, sp$v2_rank_stab, use = "complete.obs")))

# Agreement in top tiers
for (cutoff in c(15, 30, 50)) {
  v1_top <- sp$player_id[sp$v1_rank_stab <= cutoff]
  v2_top <- sp$player_id[sp$v2_rank_stab <= cutoff]
  overlap <- length(intersect(v1_top, v2_top))
  cat(sprintf("  Top-%d overlap: %d/%d (%.0f%%)\n", cutoff, overlap, cutoff, 100 * overlap / cutoff))
}

cat("\nDone.\n")
