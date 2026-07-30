#!/usr/bin/env Rscript
# derive_streamonator_pf_lenses.R
#
# Phase 0 of the iPF x Streamonator validation (July 2026).
# Enriches the cached starts_YYYY.csv files with:
#   1. The four July-2026 iPF lens indices (Overall / BACON / HR / Carry),
#      joined era-correct via team_park_era_audit.csv
#   2. Starter HR allowed per game (extracted from cached boxscore .rds files;
#      cached to starter_hr_by_game.csv so the 12k-file scan runs once)
#   3. Authoritative GSM (0-4, no Win, WHIP <= 1.18, sliding ER) and
#      blow-up flags (ER >= 5 primary, ER >= 6 robustness)
#
# Note this script DOES compute the authoritative GSM (0-4, no Win, sliding ER,
# WHIP <= 1.18), unlike the derive_streamonator_weights*.R scripts whose stored
# good_start_score column is the stale 0-5 version. starts_with_pf_lenses.csv is
# therefore safe to read directly on GSM.
#
# Consumed by: streamonator_lens_ladder.R (Phase 1). Run this first.
#
# Output: data/processed/streamonator_weight_analysis/starts_with_pf_lenses.csv
# Fully cached — no API calls. Usage: Rscript scripts/validation_tuning/derive_streamonator_pf_lenses.R

CACHE   <- file.path("data", "processed", "streamonator_weight_analysis")
PF_ERA  <- file.path("data", "processed", "park_factors", "park_factors_savant_style_with_id.csv")
ERA_MAP <- file.path("data", "processed", "park_factors", "team_park_era_audit.csv")
HR_CACHE <- file.path(CACHE, "starter_hr_by_game.csv")
OUT      <- file.path(CACHE, "starts_with_pf_lenses.csv")
SEASONS  <- 2021:2025

`%||%` <- function(a, b) if (is.null(a)) b else a

# ── 1. Starter HR allowed per game (build cache from boxscores if missing) ────
if (!file.exists(HR_CACHE)) {
  message("Building starter HR cache from boxscore .rds files (one-time, ~2 min)...")
  files <- list.files(CACHE, pattern = "^box202[1-5]_\\d+\\.rds$", full.names = TRUE)
  rows <- vector("list", 2L * length(files))
  k <- 0L
  for (f in files) {
    base    <- basename(f)
    season  <- as.integer(substr(base, 4, 7))
    game_pk <- as.integer(sub("^box\\d+_(\\d+)\\.rds$", "\\1", base))
    bx <- tryCatch(readRDS(f), error = function(e) NULL)
    if (is.null(bx) || is.null(bx$teams)) next
    for (side in c("home", "away")) {
      tm <- bx$teams[[side]]
      if (is.null(tm)) next
      sid <- suppressWarnings(as.integer(unlist(tm$pitchers)[1]))
      if (length(sid) == 0 || is.na(sid)) next
      pp  <- tm$players[[paste0("ID", sid)]]
      shr <- suppressWarnings(as.numeric(pp$stats$pitching$homeRuns %||% NA))
      k <- k + 1L
      rows[[k]] <- data.frame(season = season, game_pk = game_pk,
                              pitcher_id = sid, hr_allowed = shr)
    }
  }
  hr_df <- do.call(rbind, rows[seq_len(k)])
  write.csv(hr_df, HR_CACHE, row.names = FALSE)
  message(sprintf("  starter HR cache: %d rows", nrow(hr_df)))
} else {
  hr_df <- read.csv(HR_CACHE, stringsAsFactors = FALSE)
  message(sprintf("Starter HR cache loaded: %d rows", nrow(hr_df)))
}

# ── 2. Load starts + recompute authoritative GSM ──────────────────────────────
st <- do.call(rbind, lapply(SEASONS, function(yr) {
  f <- file.path(CACHE, sprintf("starts_%d.csv", yr))
  if (!file.exists(f)) { message(sprintf("  MISSING: %s", f)); return(NULL) }
  df <- read.csv(f, stringsAsFactors = FALSE)
  df$season <- yr
  df
}))
message(sprintf("Starts loaded: %d (%d-%d)", nrow(st), min(SEASONS), max(SEASONS)))

st$whip <- ifelse(!is.na(st$ip) & st$ip > 0, (st$h + st$bb) / st$ip, Inf)
st$gsm <- as.integer(!is.na(st$ip) & st$ip >= 5.0) +
  as.integer(!is.na(st$k) & !is.na(st$ip) & st$k >= (floor(st$ip) - 1)) +
  as.integer(!is.na(st$er) & !is.na(st$ip) & (
    (st$ip >= 6 & st$er <= 2) | (st$ip >= 5 & st$ip < 6 & st$er <= 3) |
    (st$ip >= 4 & st$ip < 5 & st$er <= 2) | (st$ip < 4 & st$er <= 1))) +
  as.integer(!is.na(st$whip) & st$whip <= 1.18)
st$good_start  <- st$gsm >= 3
st$blowup_er5  <- !is.na(st$er) & st$er >= 5
st$blowup_er6  <- !is.na(st$er) & st$er >= 6

# ── 3. Join the four iPF lenses (era-correct) ─────────────────────────────────
pf  <- read.csv(PF_ERA, stringsAsFactors = FALSE)
era <- read.csv(ERA_MAP, stringsAsFactors = FALSE)
# Primary venue per team-season = era row with most BBE (drops neutral sites)
era <- era[order(era$season, era$home_team, -era$n_bbe), ]
era <- era[!duplicated(era[, c("season", "home_team")]), ]

st$park_team   <- ifelse(st$home_away == "H", st$pitcher_team, st$opponent_team)
st$park_era_id <- era$park_era_id[match(paste(st$season, st$park_team),
                                        paste(era$season, era$home_team))]
pidx <- match(st$park_era_id, pf$park_era_id)
st$pf_overall_idx <- pf$overall_pf_idx_100[pidx]
st$pf_bacon_idx   <- pf$bacon_idx_100[pidx]
st$pf_hr_idx      <- pf$hr_idx_100[pidx]
st$pf_carry_idx   <- pf$carry_idx_100[pidx]
message(sprintf("iPF lens match: %.1f%%", 100 * mean(!is.na(st$pf_overall_idx))))

# ── 4. Join starter HR allowed ────────────────────────────────────────────────
st$hr_allowed <- hr_df$hr_allowed[match(paste(st$game_pk, st$pitcher_id),
                                        paste(hr_df$game_pk, hr_df$pitcher_id))]
message(sprintf("Starter HR match: %.1f%%", 100 * mean(!is.na(st$hr_allowed))))

# ── 5. Write ──────────────────────────────────────────────────────────────────
write.csv(st, OUT, row.names = FALSE)
message(sprintf("Wrote %s (%d rows, %d cols)", OUT, nrow(st), ncol(st)))
message(sprintf("Validation sample (non-placeholder): %d | good %.1f%% | ER5+ %.1f%% | ER6+ %.1f%%",
  sum(!st$spz_placeholder),
  100 * mean(st$good_start[!st$spz_placeholder]),
  100 * mean(st$blowup_er5[!st$spz_placeholder]),
  100 * mean(st$blowup_er6[!st$spz_placeholder])))
