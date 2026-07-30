#!/usr/bin/env Rscript
# ===========================================================================
# SP Skillz Empirical Validation & Weight Derivation
# ===========================================================================
# Purpose: Rigorous testing of SP Skillz metric candidates to determine:
#   1) Which metrics are individually predictive of future outcomes
#   2) Which combinations maximize predictive power
#   3) Optimal weights via ridge regression with cross-validation
#   4) Whether IP-tier paradigms (low/mid/high) are justified
#
# Targets: future K%, future SIERA, future WHIP, future ERA
# Sanity check: fantasy $/IP (ERA + WHIP + K, equal weights, W=0 SV=0)
# Time horizons: 1H->2H (same season), Year-over-Year
# Data: 2021-2025 (post-2020 only; Stuff+/Pitching+ available throughout)
# Metric candidates: siera, xfip, k_minus_bb_pct, ball_pct, stuff_plus,
#                    pitching_plus, whiff_pct, z_contact_pct, high_gb_flag
#
# ── WHERE THIS SITS ───────────────────────────────────────────────────────
#   This is the EXPLORATORY predecessor (largest file in the folder at ~1,300
#   lines). It screens the wide candidate list and tests whether IP-tier
#   paradigms are justified. The surviving design was then re-derived cleanly
#   in derive_sp_skillz_weights_v2.R, which is what actually produces the
#   LIVE weights.
#   Practical rule: for "what does the model use today?", read
#   derive_sp_skillz_weights_v2.R. Read this one for why candidates were cut.
#
#   Note the candidate list here includes siera, xfip and z_contact_pct, none
#   of which survive into the shipped 7-metric v2 set — that attrition is the
#   useful content of this file.
#
# ⚠ NO VERDICT RECORDED beyond that. I have no stored per-metric results for
#   this run, so nothing is asserted here about individual screen outcomes or
#   the IP-tier question. Re-run for specifics.
#
# ── RELATED DATA NOTE (saves a needless refetch) ───────────────────────────
#   The sp_skillz_validation_cache JSONs already carry per-pitch Stuff+
#   (sp_s_* columns) plus pfx mix / velo / movement. Arsenal-flavoured research
#   against this cache does NOT need a new fetch.
# ===========================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(glmnet)  # ridge regression
  library(jsonlite)
})

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

SEASONS <- 2021:2025
ASG_DATES <- list(
  "2021" = "2021-07-13",
  "2022" = "2022-07-19", "2023" = "2023-07-11", "2024" = "2024-07-16",
  "2025" = "2025-07-15"
)

# IP tier boundaries
LOW_IP_MAX <- 80
MID_IP_MAX <- 100  # mid = 80-100; high = 100+

# Minimum thresholds for inclusion
MIN_TBF_FULL <- 100
MIN_TBF_HALF <- 50
MIN_IP_NEXT_YEAR <- 40
START_SHARE_MIN <- 2/3

# Metric candidates (skill metrics only; tbf/ip_per_gs are infrastructure)
# All candidates pass YoY autocorrelation test (r >= 0.577 = repeatable skill)
# Dropped: contact_pct, swstr_pct, o_contact_pct (redundant with whiff_pct)
# Dropped: hr_fb_pct (r=0.09 YoY stability — noise, not skill; xFIP thesis)
# Dropped: iffb_pct (r=0.25 YoY stability — not repeatable)
# Dropped: gb_pct continuous (noisy below 55%; replaced with binary flag)
# high_gb_flag: binary >=55% GB — stable weak-contact skill via extreme low LA
METRIC_CANDIDATES <- c(
  "siera", "xfip", "k_minus_bb_pct",
  "ball_pct", "stuff_plus", "pitching_plus",
  "whiff_pct", "z_contact_pct", "high_gb_flag"
)

# Direction: positive means "higher is better for pitcher quality"
METRIC_QUALITY_SIGN <- c(
  siera = -1, xfip = -1, k_minus_bb_pct = 1,
  ball_pct = -1, stuff_plus = 1, pitching_plus = 1,
  whiff_pct = 1, z_contact_pct = -1, high_gb_flag = 1
)

# Target direction: positive means "higher is better outcome"
TARGET_SIGN <- c(
  next_k_pct = 1, next_siera = -1, next_whip = -1, next_era = -1
)

# Output directory
OUTPUT_DIR <- file.path("data", "processed", "sp_skillz_validation")
dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

# Cache directory for raw fetches
CACHE_DIR <- file.path("data", "raw", "sp_skillz_validation_cache")
dir.create(CACHE_DIR, recursive = TRUE, showWarnings = FALSE)

cat("=" |> rep(70) |> paste(collapse = ""), "\n")
cat("SP SKILLZ EMPIRICAL VALIDATION\n")
cat("=" |> rep(70) |> paste(collapse = ""), "\n")
cat(sprintf("Seasons: %s\n", paste(SEASONS, collapse = ", ")))
cat(sprintf("Metrics: %d candidates\n", length(METRIC_CANDIDATES)))
cat(sprintf("Targets: future K%%, SIERA, WHIP, ERA\n"))
cat(sprintf("Output: %s\n", OUTPUT_DIR))
cat("=" |> rep(70) |> paste(collapse = ""), "\n\n")

# ===========================================================================
# SECTION 1: DATA FETCHING
# ===========================================================================

cat("SECTION 1: Data Fetching\n")
cat("-" |> rep(40) |> paste(collapse = ""), "\n")

# ---------------------------------------------------------------------------
# 1A: Fangraphs full-season data
# ---------------------------------------------------------------------------
# FG API: leaders/major-league/data with plate discipline + Stuff+/Pitching+
# Key columns needed: K%, SIERA, xFIP, K-BB%, Contact%, SwStr%, Ball%,
#                     Stuff+, Pitching+, Z-Contact%, O-Contact%, Age,
#                     IP, GS, Start-IP, Relief-IP, ERA, WHIP, TBF, Pitches

fetch_fg_season <- function(season, split = "full") {
  # split: "full" (month=0), "1h" (startdate approach), "2h" (startdate approach)
  # NOTE: FG's month=32 is broken (returns ASG day only). Use date ranges instead.

  # Use a v2 cache key for date-range-based fetches to avoid stale month=32 data

  cache_file <- file.path(CACHE_DIR, sprintf("fg_%s_%d_v2.json", split, season))
  if (file.exists(cache_file)) {
    cat(sprintf("  [cache] FG %s %d\n", split, season))
    return(jsonlite::fromJSON(cache_file, flatten = TRUE)$data)
  }

  # Build URL based on split type
  type_codes <- "c,4,5,11,24,36,37,40,41,42,43,44,45,46,120,121,217,218,259,322,323"

  if (split == "full") {
    url <- sprintf(
      paste0(
        "https://www.fangraphs.com/api/leaders/major-league/data",
        "?pos=all&stats=pit&lg=all&qual=0&season=%d&month=0",
        "&season1=%d&ind=0&team=0&pageitems=2000&pagenum=1",
        "&type=%s"
      ),
      season, season, type_codes
    )
  } else {
    # Use startdate/enddate with ASG dates for proper 1H/2H splits
    asg <- ASG_DATES[[as.character(season)]]
    if (is.null(asg)) {
      cat(sprintf("  [skip] No ASG date for %d\n", season))
      return(NULL)
    }
    if (split == "1h") {
      startdate <- sprintf("%d-03-01", season)
      enddate <- asg  # up to and including ASG day
    } else {  # "2h"
      asg_next <- as.character(as.Date(asg) + 1)
      startdate <- asg_next
      enddate <- sprintf("%d-11-01", season)
    }
    url <- sprintf(
      paste0(
        "https://www.fangraphs.com/api/leaders/major-league/data",
        "?pos=all&stats=pit&lg=all&qual=0&season=%d&month=0",
        "&season1=%d&ind=0&team=0&pageitems=2000&pagenum=1",
        "&startdate=%s&enddate=%s",
        "&type=%s"
      ),
      season, season, startdate, enddate, type_codes
    )
  }

  cat(sprintf("  [fetch] FG %s %d ... ", split, season))
  resp <- tryCatch(
    jsonlite::fromJSON(url, flatten = TRUE),
    error = function(e) {
      cat(sprintf("FAILED: %s\n", conditionMessage(e)))
      return(NULL)
    }
  )

  if (is.null(resp) || !("data" %in% names(resp))) {
    cat("no data field\n")
    return(NULL)
  }

  # Cache it
  jsonlite::write_json(resp, cache_file, auto_unbox = TRUE)
  cat(sprintf("OK (%d rows)\n", nrow(resp$data)))

  Sys.sleep(2)  # polite rate limiting
  resp$data
}

# ---------------------------------------------------------------------------
# 1B: Savant Whiff% (pitch-level aggregates via statcast_search/csv)
# ---------------------------------------------------------------------------

fetch_savant_whiff <- function(season, date_gt, date_lt, split_label) {
  cache_file <- file.path(CACHE_DIR, sprintf("savant_whiff_%s_%d.csv", split_label, season))
  if (file.exists(cache_file)) {
    cat(sprintf("  [cache] Savant whiff %s %d\n", split_label, season))
    return(read_csv(cache_file, show_col_types = FALSE))
  }

  url <- sprintf(
    paste0(
      "https://baseballsavant.mlb.com/statcast_search/csv",
      "?hfGT=R%%7C&hfSea=%d%%7C&player_type=pitcher",
      "&game_date_gt=%s&game_date_lt=%s",
      "&group_by=name&min_pitches=0&min_results=0&min_pas=0",
      "&sort_col=pitches&sort_order=desc",
      "&chk_stats_whiffs=on&chk_stats_swings=on&chk_stats_pitches=on"
    ),
    season, date_gt, date_lt
  )

  cat(sprintf("  [fetch] Savant whiff %s %d ... ", split_label, season))
  tmp <- tempfile(fileext = ".csv")
  tryCatch({
    download.file(url, tmp, quiet = TRUE, mode = "wb")
    d <- read_csv(tmp, show_col_types = FALSE)
    if (nrow(d) == 0) {
      cat("empty\n")
      return(NULL)
    }
    write_csv(d, cache_file)
    cat(sprintf("OK (%d rows)\n", nrow(d)))
    Sys.sleep(3)  # savant rate limiting
    d
  }, error = function(e) {
    cat(sprintf("FAILED: %s\n", conditionMessage(e)))
    NULL
  })
}

# ---------------------------------------------------------------------------
# 1C: Fetch all data
# ---------------------------------------------------------------------------

cat("\nFetching FG full-season data...\n")
fg_full_list <- lapply(SEASONS, function(s) {
  d <- fetch_fg_season(s, "full")
  if (!is.null(d)) d$season <- s
  d
})

cat("\nFetching FG 1H data...\n")
fg_1h_list <- lapply(SEASONS, function(s) {
  d <- fetch_fg_season(s, "1h")
  if (!is.null(d)) d$season <- s
  d
})

cat("\nFetching FG 2H data...\n")
fg_2h_list <- lapply(SEASONS, function(s) {
  d <- fetch_fg_season(s, "2h")
  if (!is.null(d)) d$season <- s
  d
})

cat("\nFetching Savant Whiff% (full season)...\n")
savant_full_list <- lapply(SEASONS, function(s) {
  asg <- ASG_DATES[[as.character(s)]]
  d <- fetch_savant_whiff(s, sprintf("%d-03-01", s), sprintf("%d-11-30", s), "full")
  if (!is.null(d)) d$season <- s
  d
})

cat("\nFetching Savant Whiff% (1H)...\n")
savant_1h_list <- lapply(SEASONS, function(s) {
  asg <- ASG_DATES[[as.character(s)]]
  d <- fetch_savant_whiff(s, sprintf("%d-03-01", s), asg, "1h")
  if (!is.null(d)) d$season <- s
  d
})

cat("\nFetching Savant Whiff% (2H)...\n")
savant_2h_list <- lapply(SEASONS, function(s) {
  asg <- ASG_DATES[[as.character(s)]]
  # day after ASG through end of season
  asg_date <- as.Date(asg)
  d <- fetch_savant_whiff(s, as.character(asg_date + 1), sprintf("%d-11-30", s), "2h")
  if (!is.null(d)) d$season <- s
  d
})

# ===========================================================================
# SECTION 2: DATA ASSEMBLY
# ===========================================================================

cat("\n\nSECTION 2: Data Assembly\n")
cat("-" |> rep(40) |> paste(collapse = ""), "\n")

# ---------------------------------------------------------------------------
# 2A: Parse FG data into standardized format
# ---------------------------------------------------------------------------

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

nc <- function(x) suppressWarnings(as.numeric(x))

parse_fg_data <- function(raw, season_val) {
  if (is.null(raw) || nrow(raw) == 0) return(NULL)

  # FG column names vary; try common variants
  get_col <- function(candidates) {
    for (c in candidates) {
      if (c %in% names(raw)) return(raw[[c]])
    }
    rep(NA, nrow(raw))
  }

  data.frame(
    player_id = as.integer(get_col(c("xMLBAMID", "MLBAMID", "playerid"))),
    player_name = as.character(get_col(c("PlayerName", "Name", "name"))),
    season = season_val,
    age = nc(get_col(c("Age", "age"))),
    team = as.character(get_col(c("Team", "team"))),
    ip = ipb(get_col(c("IP", "ip"))),
    gs = nc(get_col(c("GS", "gs"))),
    tbf = nc(get_col(c("TBF", "tbf", "BF"))),
    pitches = nc(get_col(c("Pitches", "pitches", "Pit"))),
    start_ip = ipb(get_col(c("Start-IP", "start_ip"))),
    relief_ip = ipb(get_col(c("Relief-IP", "relief_ip"))),
    era = nc(get_col(c("ERA", "era"))),
    whip = nc(get_col(c("WHIP", "whip"))),
    k_pct = nc(get_col(c("K%", "k_pct", "K_pct"))),
    bb_pct = nc(get_col(c("BB%", "bb_pct", "BB_pct"))),
    siera = nc(get_col(c("SIERA", "siera"))),
    xfip = nc(get_col(c("xFIP", "xfip"))),
    k_minus_bb_pct = nc(get_col(c("K-BB%", "k_minus_bb_pct"))),
    z_contact_pct = nc(get_col(c("Z-Contact%", "z_contact_pct"))),
    balls = nc(get_col(c("Balls", "balls"))),
    stuff_plus = nc(get_col(c("sp_stuff", "Stuff+", "stuff_plus"))),
    pitching_plus = nc(get_col(c("sp_pitching", "Pitching+", "pitching_plus"))),
    gb_pct = nc(get_col(c("GB%", "gb_pct"))),  # used to derive high_gb_flag
    stringsAsFactors = FALSE
  )
}

# Parse all FG datasets
fg_full <- bind_rows(lapply(seq_along(SEASONS), function(i) {
  parse_fg_data(fg_full_list[[i]], SEASONS[i])
}))
fg_1h <- bind_rows(lapply(seq_along(SEASONS), function(i) {
  parse_fg_data(fg_1h_list[[i]], SEASONS[i])
}))
fg_2h <- bind_rows(lapply(seq_along(SEASONS), function(i) {
  parse_fg_data(fg_2h_list[[i]], SEASONS[i])
}))

cat(sprintf("  FG full-season: %d rows across %d seasons\n", nrow(fg_full), n_distinct(fg_full$season)))
cat(sprintf("  FG 1H: %d rows\n", nrow(fg_1h)))
cat(sprintf("  FG 2H: %d rows\n", nrow(fg_2h)))

# ---------------------------------------------------------------------------
# 2B: Parse Savant Whiff% and merge
# ---------------------------------------------------------------------------

parse_savant_whiff <- function(raw, season_val) {
  if (is.null(raw) || nrow(raw) == 0) return(NULL)

  get_col <- function(candidates) {
    for (c in candidates) {
      if (c %in% names(raw)) return(raw[[c]])
    }
    rep(NA, nrow(raw))
  }

  d <- data.frame(
    player_id = as.integer(get_col(c("player_id", "pitcher"))),
    season = season_val,
    whiffs = nc(get_col(c("whiffs", "Whiffs"))),
    swings = nc(get_col(c("swings", "Swings"))),
    stringsAsFactors = FALSE
  )
  d$whiff_pct <- ifelse(!is.na(d$swings) & d$swings > 0,
                        100 * d$whiffs / d$swings, NA_real_)
  d[!is.na(d$player_id), ]
}

savant_full <- bind_rows(lapply(seq_along(SEASONS), function(i) {
  parse_savant_whiff(savant_full_list[[i]], SEASONS[i])
}))
savant_1h <- bind_rows(lapply(seq_along(SEASONS), function(i) {
  parse_savant_whiff(savant_1h_list[[i]], SEASONS[i])
}))
savant_2h <- bind_rows(lapply(seq_along(SEASONS), function(i) {
  parse_savant_whiff(savant_2h_list[[i]], SEASONS[i])
}))

cat(sprintf("  Savant full: %d rows\n", nrow(savant_full)))
cat(sprintf("  Savant 1H: %d rows\n", nrow(savant_1h)))
cat(sprintf("  Savant 2H: %d rows\n", nrow(savant_2h)))

# ---------------------------------------------------------------------------
# 2C: Compute derived columns & merge Savant into FG
# ---------------------------------------------------------------------------

compute_derived <- function(df) {
  # ball_pct from balls/pitches if not directly available
  df$ball_pct <- ifelse(!is.na(df$pitches) & df$pitches > 0 & !is.na(df$balls),
                        100 * df$balls / df$pitches, NA_real_)

  # start_share
  has_split <- !is.na(df$start_ip) | !is.na(df$relief_ip)
  df$start_ip[has_split & is.na(df$start_ip)] <- 0
  df$relief_ip[has_split & is.na(df$relief_ip)] <- 0
  df$start_share <- ifelse(
    has_split & (df$start_ip + df$relief_ip) > 0,
    df$start_ip / (df$start_ip + df$relief_ip),
    NA_real_
  )
  # GS/G fallback
  needs_fb <- is.na(df$start_share) & !is.na(df$gs) & !is.na(df$ip) & df$ip > 0
  if (any(needs_fb, na.rm = TRUE)) {
    # estimate: if gs > 0 and gs makes up majority of appearances
    df$start_share[needs_fb] <- ifelse(df$gs[needs_fb] > 0, 1, 0)
  }

  # high_gb_flag: binary indicator for elite groundball skill (>=55% GB)
  # GB% comes in as 0-1 from FG API
  df$high_gb_flag <- as.integer(!is.na(df$gb_pct) & df$gb_pct >= 0.55)

  df
}

merge_savant <- function(fg_df, savant_df) {
  if (is.null(savant_df) || nrow(savant_df) == 0) return(fg_df)
  savant_slim <- savant_df[, c("player_id", "season", "whiff_pct"), drop = FALSE]
  # deduplicate savant

  savant_slim <- savant_slim[!duplicated(savant_slim[, c("player_id", "season")]), ]
  merge(fg_df, savant_slim, by = c("player_id", "season"), all.x = TRUE)
}

# Apply
fg_full <- compute_derived(fg_full)
fg_full <- merge_savant(fg_full, savant_full)

fg_1h <- compute_derived(fg_1h)
fg_1h <- merge_savant(fg_1h, savant_1h)

fg_2h <- compute_derived(fg_2h)
fg_2h <- merge_savant(fg_2h, savant_2h)

# ---------------------------------------------------------------------------
# 2D: Identify SP pool from full-season data, then pull from splits
# ---------------------------------------------------------------------------
# Full-season determines the SP pool (start_share >= 2/3, TBF >= 100).
# 1H and 2H data is unfiltered — we just look up pool members in each half.
# Players must appear in BOTH halves to be included in 1H→2H analysis.

filter_starters <- function(df, min_tbf) {
  df %>%
    filter(
      !is.na(start_share) & start_share >= START_SHARE_MIN,
      !is.na(tbf) & tbf >= min_tbf
    )
}

sp_full <- filter_starters(fg_full, MIN_TBF_FULL)

# SP pool: player_id × season combos that qualify as starters
sp_pool <- sp_full %>% select(player_id, season) %>% distinct()

# Pull those players from unfiltered 1H and 2H data (no TBF/start_share filter)
sp_1h <- fg_1h %>%
  inner_join(sp_pool, by = c("player_id", "season"))
sp_2h <- fg_2h %>%
  inner_join(sp_pool, by = c("player_id", "season"))

cat(sprintf("\n  SP pool (full-season qualified): %d player-seasons\n", nrow(sp_pool)))
cat(sprintf("  SP pool members found in 1H data: %d\n", nrow(sp_1h)))
cat(sprintf("  SP pool members found in 2H data: %d\n", nrow(sp_2h)))
cat(sprintf("  SP pool members in BOTH halves: %d\n",
            nrow(inner_join(
              sp_1h %>% select(player_id, season),
              sp_2h %>% select(player_id, season),
              by = c("player_id", "season")
            ))))

# ---------------------------------------------------------------------------
# 2E: Build paired datasets for prediction testing
# ---------------------------------------------------------------------------

# YoY pairs: season N metrics → season N+1 outcomes
build_yoy_pairs <- function(base_df, target_df) {
  base <- base_df %>%
    select(player_id, season, all_of(intersect(METRIC_CANDIDATES, names(base_df))),
           start_ip, tbf, pitches)
  target <- target_df %>%
    select(player_id, season, k_pct, siera, whip, era, ip) %>%
    rename(next_k_pct = k_pct, next_siera = siera, next_whip = whip,
           next_era = era, next_ip = ip) %>%
    mutate(season = season - 1L)  # align: target's season-1 = base's season

  pairs <- inner_join(base, target, by = c("player_id", "season"))
  pairs %>% filter(!is.na(next_ip) & next_ip >= MIN_IP_NEXT_YEAR)
}

# 1H→2H pairs: 1H metrics → 2H outcomes (same season)
# Uses proper date-range-based 2H data. Inner join ensures player appears in both.
build_half_pairs <- function(first_half_df, second_half_df) {
  base <- first_half_df %>%
    select(player_id, season, all_of(intersect(METRIC_CANDIDATES, names(first_half_df))),
           start_ip, tbf, pitches)
  target <- second_half_df %>%
    select(player_id, season, k_pct, siera, whip, era, ip) %>%
    rename(next_k_pct = k_pct, next_siera = siera, next_whip = whip,
           next_era = era, next_ip = ip)

  # Inner join: only players with data in BOTH halves
  inner_join(base, target, by = c("player_id", "season"))
}

yoy_pairs <- build_yoy_pairs(sp_full, sp_full)
half_pairs <- build_half_pairs(sp_1h, sp_2h)

cat(sprintf("\n  YoY pairs: %d (across %d season transitions)\n",
            nrow(yoy_pairs), n_distinct(yoy_pairs$season)))
cat(sprintf("  1H->2H pairs: %d (across %d seasons)\n",
            nrow(half_pairs), n_distinct(half_pairs$season)))

# Assign IP tiers based on full-season start_ip
# For YoY pairs, start_ip is already full-season (from sp_full)
# For half_pairs, start_ip is 1H only — use full-season lookup instead
assign_ip_tier <- function(df, ip_col = "start_ip") {
  df %>% mutate(
    ip_tier = case_when(
      .data[[ip_col]] <= LOW_IP_MAX ~ "low_ip",
      .data[[ip_col]] <= MID_IP_MAX ~ "mid_ip",
      TRUE ~ "high_ip"
    )
  )
}

yoy_pairs <- assign_ip_tier(yoy_pairs)

# For half_pairs, join in full-season start_ip for tier assignment
half_pairs <- half_pairs %>%
  left_join(
    sp_full %>% select(player_id, season, full_start_ip = start_ip),
    by = c("player_id", "season")
  ) %>%
  assign_ip_tier(ip_col = "full_start_ip")

cat(sprintf("  YoY tier distribution: low=%d, mid=%d, high=%d\n",
            sum(yoy_pairs$ip_tier == "low_ip", na.rm = TRUE),
            sum(yoy_pairs$ip_tier == "mid_ip", na.rm = TRUE),
            sum(yoy_pairs$ip_tier == "high_ip", na.rm = TRUE)))

# ===========================================================================
# SECTION 3: INDIVIDUAL METRIC CORRELATIONS
# ===========================================================================

cat("\n\nSECTION 3: Individual Metric Correlations\n")
cat("-" |> rep(40) |> paste(collapse = ""), "\n")

# For each metric × target, compute correlation (overall and by tier)
compute_individual_cors <- function(pairs_df, horizon_label) {
  targets <- names(TARGET_SIGN)
  available_metrics <- intersect(METRIC_CANDIDATES, names(pairs_df))

  results <- list()
  tiers <- c("all", "low_ip", "mid_ip", "high_ip")

  for (tier in tiers) {
    df <- if (tier == "all") pairs_df else pairs_df[pairs_df$ip_tier == tier, ]
    if (nrow(df) < 20) next

    for (target in targets) {
      if (!target %in% names(df)) next
      for (metric in available_metrics) {
        ok <- !is.na(df[[metric]]) & !is.na(df[[target]])
        n <- sum(ok)
        if (n < 20) next

        r <- cor(df[[metric]][ok], df[[target]][ok])
        # "quality-aligned r": positive means metric predicts good outcomes
        aligned_r <- r * TARGET_SIGN[[target]] * METRIC_QUALITY_SIGN[[metric]]

        results[[length(results) + 1]] <- data.frame(
          horizon = horizon_label,
          tier = tier,
          target = target,
          metric = metric,
          n = n,
          r = round(r, 4),
          r2 = round(r^2, 4),
          aligned_r = round(aligned_r, 4),
          stringsAsFactors = FALSE
        )
      }
    }
  }

  bind_rows(results)
}

cors_yoy <- compute_individual_cors(yoy_pairs, "yoy")
cors_half <- compute_individual_cors(half_pairs, "1h_2h")
individual_cors <- bind_rows(cors_yoy, cors_half)

# Save
write_csv(individual_cors, file.path(OUTPUT_DIR, "individual_metric_correlations.csv"))

# Print summary: best metrics per target (overall)
cat("\n  Top metrics by aligned R (YoY, all tiers):\n")
cors_yoy %>%
  filter(.data$tier == "all") %>%
  group_by(.data$target) %>%
  arrange(desc(.data$aligned_r)) %>%
  slice_head(n = 5) %>%
  mutate(display = sprintf("    %s → %s: r=%.3f, R²=%.3f (n=%d)",
                           metric, target, aligned_r, r2, n)) %>%
  pull(display) %>%
  cat(sep = "\n")

cat("\n\n  Top metrics by aligned R (1H→Full, all tiers):\n")
if (nrow(cors_half) > 0) {
  cors_half %>%
    filter(.data$tier == "all") %>%
    group_by(.data$target) %>%
    arrange(desc(.data$aligned_r)) %>%
    slice_head(n = 5) %>%
    mutate(display = sprintf("    %s → %s: r=%.3f, R²=%.3f (n=%d)",
                             metric, target, aligned_r, r2, n)) %>%
    pull(display) %>%
    cat(sep = "\n")
} else {
  cat("  (no 1H→Full data available)\n")
}

# ===========================================================================
# SECTION 4: COMBINATORIAL SUBSET TESTING
# ===========================================================================

cat("\n\nSECTION 4: Combinatorial Subset Testing\n")
cat("-" |> rep(40) |> paste(collapse = ""), "\n")

# For each subset of metrics, run ridge regression predicting each target
# with leave-one-year-out cross-validation.

# Helper: run ridge regression (or OLS for single predictors) with LOYO-CV.
# Returns mean out-of-sample R² across held-out seasons.
ridge_cv_r2 <- function(pairs_df, metric_cols, target_col) {
  # Remove rows with any NA in metric_cols or target
  complete <- pairs_df %>%
    select(season, all_of(metric_cols), all_of(target_col)) %>%
    filter(complete.cases(.))

  if (nrow(complete) < 30) return(NA_real_)

  seasons <- sort(unique(complete$season))
  if (length(seasons) < 3) return(NA_real_)

  use_ols <- (length(metric_cols) == 1)  # glmnet requires p >= 2

  cv_r2s <- numeric(length(seasons))

  for (i in seq_along(seasons)) {
    test_season <- seasons[i]
    train <- complete[complete$season != test_season, ]
    test <- complete[complete$season == test_season, ]

    if (nrow(train) < 20 || nrow(test) < 10) {
      cv_r2s[i] <- NA_real_
      next
    }

    X_train <- as.matrix(train[, metric_cols, drop = FALSE])
    y_train <- train[[target_col]]
    X_test <- as.matrix(test[, metric_cols, drop = FALSE])
    y_test <- test[[target_col]]

    # Standardize using training set parameters
    means <- colMeans(X_train, na.rm = TRUE)
    sds <- apply(X_train, 2, sd, na.rm = TRUE)
    sds[sds == 0] <- 1

    X_train_scaled <- scale(X_train, center = means, scale = sds)
    X_test_scaled <- scale(X_test, center = means, scale = sds)

    if (use_ols) {
      # Simple OLS for single-predictor models
      fit <- tryCatch(
        lm(y ~ x, data = data.frame(y = y_train, x = X_train_scaled[, 1])),
        error = function(e) NULL
      )
      if (is.null(fit)) { cv_r2s[i] <- NA_real_; next }
      preds <- predict(fit, newdata = data.frame(x = X_test_scaled[, 1]))
    } else {
      # Ridge regression for multi-predictor models
      fit <- tryCatch(
        cv.glmnet(X_train_scaled, y_train, alpha = 0, nfolds = 5),
        error = function(e) NULL
      )
      if (is.null(fit)) { cv_r2s[i] <- NA_real_; next }
      preds <- as.numeric(predict(fit, X_test_scaled, s = "lambda.min"))
    }

    ss_res <- sum((y_test - preds)^2)
    ss_tot <- sum((y_test - mean(y_test))^2)
    cv_r2s[i] <- ifelse(ss_tot > 0, 1 - ss_res / ss_tot, NA_real_)
  }

  valid <- cv_r2s[is.finite(cv_r2s)]
  if (length(valid) == 0) return(NA_real_)
  mean(valid)
}

# Generate all subsets up to a max size, plus the full model
# For efficiency: test all singles, pairs, triples, and then
# forward-stepwise from the best triple up to full model

available_metrics_yoy <- intersect(METRIC_CANDIDATES, names(yoy_pairs))
available_metrics_yoy <- available_metrics_yoy[
  sapply(available_metrics_yoy, function(m) sum(!is.na(yoy_pairs[[m]])) >= 50)
]

cat(sprintf("  Available metrics with sufficient data: %d\n", length(available_metrics_yoy)))
cat(sprintf("  Metrics: %s\n", paste(available_metrics_yoy, collapse = ", ")))

targets_to_test <- intersect(names(TARGET_SIGN), names(yoy_pairs))
cat(sprintf("  Targets available: %s\n", paste(targets_to_test, collapse = ", ")))

# --- Singles ---
cat("\n  Testing all single-metric models...\n")
single_results <- list()
for (target in targets_to_test) {
  for (metric in available_metrics_yoy) {
    r2 <- ridge_cv_r2(yoy_pairs, metric, target)
    single_results[[length(single_results) + 1]] <- data.frame(
      target = target, metrics = metric, n_metrics = 1L,
      cv_r2 = round(r2, 4), stringsAsFactors = FALSE
    )
  }
}
singles_df <- bind_rows(single_results)
cat(sprintf("  Singles complete: %d models tested\n", nrow(singles_df)))

# --- Pairs ---
cat("  Testing all metric pairs...\n")
pair_combos <- combn(available_metrics_yoy, 2, simplify = FALSE)
cat(sprintf("  %d pair combinations to test\n", length(pair_combos)))

pair_results <- list()
for (target in targets_to_test) {
  for (combo in pair_combos) {
    r2 <- ridge_cv_r2(yoy_pairs, combo, target)
    pair_results[[length(pair_results) + 1]] <- data.frame(
      target = target, metrics = paste(combo, collapse = "+"),
      n_metrics = 2L, cv_r2 = round(r2, 4), stringsAsFactors = FALSE
    )
  }
}
pairs_df_results <- bind_rows(pair_results)
cat(sprintf("  Pairs complete: %d models tested\n", nrow(pairs_df_results)))

# --- Triples ---
cat("  Testing all metric triples...\n")
triple_combos <- combn(available_metrics_yoy, 3, simplify = FALSE)
cat(sprintf("  %d triple combinations to test\n", length(triple_combos)))

triple_results <- list()
for (target in targets_to_test) {
  for (combo in triple_combos) {
    r2 <- ridge_cv_r2(yoy_pairs, combo, target)
    triple_results[[length(triple_results) + 1]] <- data.frame(
      target = target, metrics = paste(combo, collapse = "+"),
      n_metrics = 3L, cv_r2 = round(r2, 4), stringsAsFactors = FALSE
    )
  }
}
triples_df <- bind_rows(triple_results)
cat(sprintf("  Triples complete: %d models tested\n", nrow(triples_df)))

# --- Quads through full model (forward stepwise from best triple) ---
cat("  Running forward stepwise from best triples...\n")

stepwise_results <- list()
for (target in targets_to_test) {
  # Start from the best triple for this target
  best_triple <- triples_df %>%
    filter(target == !!target) %>%
    arrange(desc(cv_r2)) %>%
    slice(1)

  current_set <- strsplit(best_triple$metrics, "\\+")[[1]]
  remaining <- setdiff(available_metrics_yoy, current_set)
  best_r2 <- best_triple$cv_r2

  stepwise_results[[length(stepwise_results) + 1]] <- data.frame(
    target = target, step = 3L,
    metrics = paste(current_set, collapse = "+"),
    added = NA_character_,
    cv_r2 = best_r2, improvement = NA_real_,
    stringsAsFactors = FALSE
  )

  while (length(remaining) > 0) {
    candidates <- list()
    for (m in remaining) {
      test_set <- c(current_set, m)
      r2 <- ridge_cv_r2(yoy_pairs, test_set, target)
      candidates[[m]] <- r2
    }

    best_add <- names(which.max(unlist(candidates)))
    new_r2 <- candidates[[best_add]]
    improvement <- new_r2 - best_r2

    current_set <- c(current_set, best_add)
    remaining <- setdiff(remaining, best_add)
    best_r2 <- new_r2

    stepwise_results[[length(stepwise_results) + 1]] <- data.frame(
      target = target, step = length(current_set),
      metrics = paste(current_set, collapse = "+"),
      added = best_add,
      cv_r2 = round(new_r2, 4),
      improvement = round(improvement, 4),
      stringsAsFactors = FALSE
    )
  }
}
stepwise_df <- bind_rows(stepwise_results)
cat(sprintf("  Stepwise complete\n"))

# Combine all subset results
all_subsets <- bind_rows(
  singles_df %>% select(target, metrics, n_metrics, cv_r2),
  pairs_df_results %>% select(target, metrics, n_metrics, cv_r2),
  triples_df %>% select(target, metrics, n_metrics, cv_r2)
)

write_csv(all_subsets, file.path(OUTPUT_DIR, "combinatorial_subset_results.csv"))
write_csv(stepwise_df, file.path(OUTPUT_DIR, "forward_stepwise_results.csv"))

# Print stepwise summary
cat("\n  Forward stepwise summary (YoY):\n")
for (target in targets_to_test) {
  cat(sprintf("\n  Target: %s\n", target))
  stepwise_df %>%
    filter(target == !!target) %>%
    mutate(display = sprintf("    Step %d: +%s → R²=%.4f (Δ=%.4f) [%s]",
                             step, ifelse(is.na(added), "start", added),
                             cv_r2, ifelse(is.na(improvement), 0, improvement),
                             metrics)) %>%
    pull(display) %>%
    cat(sep = "\n")
}

# ===========================================================================
# SECTION 5: RIDGE REGRESSION — OPTIMAL WEIGHTS
# ===========================================================================

cat("\n\nSECTION 5: Ridge Regression — Optimal Weights\n")
cat("-" |> rep(40) |> paste(collapse = ""), "\n")

# Fit ridge regression on full training data for each target × tier
# Use the best metric subsets identified above, plus the full set

derive_ridge_weights <- function(pairs_df, metric_cols, target_col, tier_filter = "all") {
  df <- if (tier_filter == "all") pairs_df else pairs_df[pairs_df$ip_tier == tier_filter, ]
  complete <- df %>%
    select(season, all_of(metric_cols), all_of(target_col)) %>%
    filter(complete.cases(.))

  if (nrow(complete) < 30) return(NULL)

  X <- as.matrix(complete[, metric_cols, drop = FALSE])
  y <- complete[[target_col]]

  # Standardize
  means <- colMeans(X, na.rm = TRUE)
  sds <- apply(X, 2, sd, na.rm = TRUE)
  sds[sds == 0] <- 1
  X_scaled <- scale(X, center = means, scale = sds)

  fit <- tryCatch(
    cv.glmnet(X_scaled, y, alpha = 0, nfolds = 10),
    error = function(e) NULL
  )
  if (is.null(fit)) return(NULL)

  coefs <- as.numeric(coef(fit, s = "lambda.min"))[-1]  # drop intercept
  names(coefs) <- metric_cols

  # LOYO CV R²
  cv_r2 <- ridge_cv_r2(df, metric_cols, target_col)

  list(
    coefficients = coefs,
    cv_r2 = cv_r2,
    lambda = fit$lambda.min,
    n = nrow(complete)
  )
}

# Run for each target × tier combination
tiers_to_test <- c("all", "low_ip", "mid_ip", "high_ip")
weight_results <- list()

for (target in targets_to_test) {
  for (tier in tiers_to_test) {
    result <- derive_ridge_weights(yoy_pairs, available_metrics_yoy, target, tier)
    if (is.null(result)) next

    weight_results[[length(weight_results) + 1]] <- data.frame(
      target = target,
      tier = tier,
      metric = names(result$coefficients),
      coefficient = round(result$coefficients, 4),
      cv_r2 = round(result$cv_r2, 4),
      n = result$n,
      lambda = round(result$lambda, 6),
      stringsAsFactors = FALSE
    )
  }
}

weights_df <- bind_rows(weight_results)
write_csv(weights_df, file.path(OUTPUT_DIR, "ridge_optimal_weights.csv"))

# Also do 1H→2H
weight_results_half <- list()
available_metrics_half <- intersect(METRIC_CANDIDATES, names(half_pairs))
available_metrics_half <- available_metrics_half[
  sapply(available_metrics_half, function(m) sum(!is.na(half_pairs[[m]])) >= 50)
]

for (target in intersect(targets_to_test, names(half_pairs))) {
  for (tier in tiers_to_test) {
    result <- derive_ridge_weights(half_pairs, available_metrics_half, target, tier)
    if (is.null(result)) next

    weight_results_half[[length(weight_results_half) + 1]] <- data.frame(
      target = target,
      tier = tier,
      metric = names(result$coefficients),
      coefficient = round(result$coefficients, 4),
      cv_r2 = round(result$cv_r2, 4),
      n = result$n,
      lambda = round(result$lambda, 6),
      stringsAsFactors = FALSE
    )
  }
}

weights_half_df <- bind_rows(weight_results_half)
write_csv(weights_half_df, file.path(OUTPUT_DIR, "ridge_optimal_weights_1h2h.csv"))

# Print weight summaries
cat("\n  Ridge weights (YoY, all tiers, all metrics):\n")
weights_df %>%
  filter(.data$tier == "all") %>%
  group_by(.data$target) %>%
  arrange(desc(abs(.data$coefficient))) %>%
  mutate(display = sprintf("    %s: coef=%.3f", metric, coefficient)) %>%
  summarise(
    cv_r2 = first(cv_r2),
    top_metrics = paste(head(display, 5), collapse = "\n"),
    .groups = "drop"
  ) %>%
  mutate(full = sprintf("\n  Target: %s (CV R²=%.4f)\n%s", target, cv_r2, top_metrics)) %>%
  pull(full) %>%
  cat(sep = "\n")

# ===========================================================================
# SECTION 6: IP TIER PARADIGM ANALYSIS
# ===========================================================================

cat("\n\nSECTION 6: IP Tier Paradigm Analysis\n")
cat("-" |> rep(40) |> paste(collapse = ""), "\n")

# Question: do optimal weights differ meaningfully across IP tiers?
# Compare ridge coefficients for low_ip vs mid_ip vs high_ip

tier_comparison <- weights_df %>%
  filter(tier != "all") %>%
  select(target, tier, metric, coefficient) %>%
  tidyr::pivot_wider(names_from = tier, values_from = coefficient, names_prefix = "coef_")

write_csv(tier_comparison, file.path(OUTPUT_DIR, "tier_weight_comparison.csv"))

cat("  Weight divergence by tier (absolute difference high_ip - low_ip):\n")
tier_comparison %>%
  filter(!is.na(coef_low_ip) & !is.na(coef_high_ip)) %>%
  mutate(divergence = abs(coef_high_ip - coef_low_ip)) %>%
  group_by(target) %>%
  arrange(desc(divergence)) %>%
  slice_head(n = 5) %>%
  mutate(display = sprintf("    %s → %s: low=%.3f, mid=%.3f, high=%.3f (Δ=%.3f)",
                           target, metric,
                           coef_low_ip,
                           ifelse(is.na(coef_mid_ip), NA_real_, coef_mid_ip),
                           coef_high_ip, divergence)) %>%
  pull(display) %>%
  cat(sep = "\n")

# Also test: does a single universal model perform comparably to tier-specific models?
cat("\n\n  Universal vs tier-specific model performance:\n")
for (target in targets_to_test) {
  universal_r2 <- ridge_cv_r2(yoy_pairs, available_metrics_yoy, target)

  tier_r2s <- sapply(c("low_ip", "mid_ip", "high_ip"), function(tier) {
    tier_data <- yoy_pairs[yoy_pairs$ip_tier == tier, ]
    if (nrow(tier_data) < 30) return(NA_real_)
    ridge_cv_r2(tier_data, available_metrics_yoy, target)
  })

  # Weighted average of tier-specific R²s
  tier_ns <- table(yoy_pairs$ip_tier[!is.na(yoy_pairs[[target]])])
  total_n <- sum(tier_ns)
  weighted_tier_r2 <- sum(tier_r2s * tier_ns[names(tier_r2s)] / total_n, na.rm = TRUE)

  cat(sprintf("  %s: universal=%.4f, tier-weighted=%.4f (low=%.4f, mid=%.4f, high=%.4f)\n",
              target, universal_r2, weighted_tier_r2,
              tier_r2s["low_ip"], tier_r2s["mid_ip"], tier_r2s["high_ip"]))
}

# ===========================================================================
# SECTION 7: MULTICOLLINEARITY CHECK
# ===========================================================================

cat("\n\nSECTION 7: Multicollinearity Analysis\n")
cat("-" |> rep(40) |> paste(collapse = ""), "\n")

# Correlation matrix among predictor metrics
cor_matrix_data <- yoy_pairs %>%
  select(all_of(available_metrics_yoy)) %>%
  filter(complete.cases(.))

if (nrow(cor_matrix_data) > 50) {
  cor_mat <- cor(cor_matrix_data, use = "complete.obs")
  write_csv(
    as.data.frame(cor_mat) %>% mutate(metric = rownames(cor_mat), .before = 1),
    file.path(OUTPUT_DIR, "metric_intercorrelation_matrix.csv")
  )

  # Flag high correlations (|r| > 0.7)
  cat("  Highly correlated metric pairs (|r| > 0.70):\n")
  for (i in 1:(ncol(cor_mat) - 1)) {
    for (j in (i + 1):ncol(cor_mat)) {
      r <- cor_mat[i, j]
      if (abs(r) > 0.70) {
        cat(sprintf("    %s × %s: r = %.3f\n",
                    rownames(cor_mat)[i], colnames(cor_mat)[j], r))
      }
    }
  }
}

# ===========================================================================
# SECTION 8: Z-CONTACT SPECIFIC ANALYSIS
# ===========================================================================

cat("\n\nSECTION 8: Z-Contact Specific Analysis\n")
cat("-" |> rep(40) |> paste(collapse = ""), "\n")

# Q4a: Is z_contact_pct predictive beyond whiff_pct?
cat("  Z-Contact individual predictiveness:\n")
individual_cors %>%
  filter(metric %in% c("z_contact_pct", "whiff_pct"),
         tier == "all") %>%
  arrange(target, desc(abs(aligned_r))) %>%
  mutate(display = sprintf("    %s → %s [%s]: aligned_r=%.3f, R²=%.3f (n=%d)",
                           metric, target, horizon, aligned_r, r2, n)) %>%
  pull(display) %>%
  cat(sep = "\n")

# Q4b: Does z_con alongside whiff improve the model?
cat("\n\n  Whiff alone vs Whiff+Z-Con:\n")
for (target in targets_to_test) {
  r2_whiff <- ridge_cv_r2(yoy_pairs, "whiff_pct", target)
  r2_whiff_zcon <- ridge_cv_r2(yoy_pairs, c("whiff_pct", "z_contact_pct"), target)

  cat(sprintf("  %s:\n", target))
  cat(sprintf("    whiff_pct alone:          R²=%.4f\n", r2_whiff))
  cat(sprintf("    whiff + z_con:            R²=%.4f (Δ=%.4f)\n", r2_whiff_zcon, r2_whiff_zcon - r2_whiff))
}

# Q4c: Full model with vs without z_contact_pct
cat("\n\n  Full model with vs without Z-Contact:\n")
for (target in targets_to_test) {
  full_set <- available_metrics_yoy
  no_zcon_set <- setdiff(available_metrics_yoy, "z_contact_pct")

  r2_full <- ridge_cv_r2(yoy_pairs, full_set, target)
  r2_no_zcon <- ridge_cv_r2(yoy_pairs, no_zcon_set, target)

  cat(sprintf("  %s: with z_con R²=%.4f, without R²=%.4f (Δ=%.4f)\n",
              target, r2_full, r2_no_zcon, r2_full - r2_no_zcon))
}

# ===========================================================================
# SECTION 9: SIERA/xFIP MARGINAL VALUE TEST
# ===========================================================================

cat("\n\nSECTION 9: SIERA/xFIP Marginal Value\n")
cat("-" |> rep(40) |> paste(collapse = ""), "\n")

# Test whether SIERA and xFIP add value beyond the skill metrics they're built from
cat("  Do SIERA/xFIP add value on top of the skill metrics?\n")
for (target in targets_to_test) {
  # Base: everything except SIERA and xFIP
  base_set <- setdiff(available_metrics_yoy, c("siera", "xfip"))

  r2_base <- ridge_cv_r2(yoy_pairs, base_set, target)
  r2_plus_siera <- ridge_cv_r2(yoy_pairs, c(base_set, "siera"), target)
  r2_plus_xfip <- ridge_cv_r2(yoy_pairs, c(base_set, "xfip"), target)
  r2_plus_both <- ridge_cv_r2(yoy_pairs, c(base_set, "siera", "xfip"), target)
  r2_full <- ridge_cv_r2(yoy_pairs, available_metrics_yoy, target)

  cat(sprintf("\n  %s:\n", target))
  cat(sprintf("    base (no estimators):   R²=%.4f\n", r2_base))
  cat(sprintf("    + siera:                R²=%.4f (Δ=%.4f)\n", r2_plus_siera, r2_plus_siera - r2_base))
  cat(sprintf("    + xfip:                 R²=%.4f (Δ=%.4f)\n", r2_plus_xfip, r2_plus_xfip - r2_base))
  cat(sprintf("    + both:                 R²=%.4f (Δ=%.4f)\n", r2_plus_both, r2_plus_both - r2_base))
  cat(sprintf("    full model (all 9):     R²=%.4f\n", r2_full))
}

# ===========================================================================
# SECTION 10: 1H→2H ANALYSIS (same tests for in-season prediction)
# ===========================================================================

cat("\n\nSECTION 10: 1H→2H Replication\n")
cat("-" |> rep(40) |> paste(collapse = ""), "\n")

# Repeat key analyses on half-season pairs
available_metrics_half <- intersect(METRIC_CANDIDATES, names(half_pairs))
available_metrics_half <- available_metrics_half[
  sapply(available_metrics_half, function(m) sum(!is.na(half_pairs[[m]])) >= 50)
]

cat(sprintf("  Available metrics for 1H→2H: %s\n", paste(available_metrics_half, collapse = ", ")))
cat(sprintf("  Available targets for 1H→2H: %s\n",
            paste(intersect(targets_to_test, names(half_pairs)), collapse = ", ")))
cat(sprintf("  half_pairs rows: %d, seasons: %s\n", nrow(half_pairs),
            paste(sort(unique(half_pairs$season)), collapse=",")))
cat(sprintf("  half_pairs season class: %s\n", class(half_pairs$season)))
# Quick single-metric test
test_complete <- half_pairs %>%
  select(season, all_of(available_metrics_half[1]), next_k_pct) %>%
  filter(complete.cases(.))
cat(sprintf("  Test complete cases (%s + next_k_pct): %d across %d seasons\n",
            available_metrics_half[1], nrow(test_complete), n_distinct(test_complete$season)))

cat("\n  Forward stepwise (1H→2H):\n")
stepwise_half_results <- list()
for (target in intersect(targets_to_test, names(half_pairs))) {
  # Find best single
  best_single_r2 <- -Inf
  best_single <- NULL
  for (m in available_metrics_half) {
    r2 <- ridge_cv_r2(half_pairs, m, target)
    if (is.finite(r2) && r2 > best_single_r2) {
      best_single_r2 <- r2
      best_single <- m
    }
  }

  if (is.null(best_single)) {
    cat(sprintf("    [skip] %s — no metric produced valid R²\n", target))
    next
  }

  current_set <- best_single
  remaining <- setdiff(available_metrics_half, current_set)
  best_r2 <- best_single_r2

  stepwise_half_results[[length(stepwise_half_results) + 1]] <- data.frame(
    target = target, step = 1L, metrics = best_single,
    added = best_single, cv_r2 = round(best_r2, 4),
    improvement = NA_real_, stringsAsFactors = FALSE
  )

  while (length(remaining) > 0) {
    candidates <- list()
    for (m in remaining) {
      r2 <- ridge_cv_r2(half_pairs, c(current_set, m), target)
      candidates[[m]] <- r2
    }
    best_add <- names(which.max(unlist(candidates)))
    new_r2 <- candidates[[best_add]]
    improvement <- new_r2 - best_r2
    current_set <- c(current_set, best_add)
    remaining <- setdiff(remaining, best_add)
    best_r2 <- new_r2

    stepwise_half_results[[length(stepwise_half_results) + 1]] <- data.frame(
      target = target, step = length(current_set),
      metrics = paste(current_set, collapse = "+"),
      added = best_add, cv_r2 = round(new_r2, 4),
      improvement = round(improvement, 4), stringsAsFactors = FALSE
    )
  }
}
stepwise_half_df <- bind_rows(stepwise_half_results)
write_csv(stepwise_half_df, file.path(OUTPUT_DIR, "forward_stepwise_1h2h.csv"))

cat("\n  1H→2H stepwise summary:\n")
if (nrow(stepwise_half_df) == 0) {
  cat("  (no valid stepwise results — ridge CV may require more data per season)\n")
} else {
  for (target in intersect(targets_to_test, names(half_pairs))) {
    cat(sprintf("\n  Target: %s\n", target))
    sub <- stepwise_half_df %>% filter(.data$target == !!target)
    if (nrow(sub) > 0) {
      sub %>%
        mutate(display = sprintf("    Step %d: +%s → R²=%.4f (Δ=%.4f)",
                                 step, added, cv_r2,
                                 ifelse(is.na(improvement), 0, improvement))) %>%
        pull(display) %>%
        cat(sep = "\n")
    }
  }
}

# ===========================================================================
# SECTION 11: FANTASY DOLLAR SANITY CHECK
# ===========================================================================

cat("\n\nSECTION 11: Fantasy Dollar Sanity Check\n")
cat("-" |> rep(40) |> paste(collapse = ""), "\n")

# Compute a simple "fantasy value per IP" based on ERA + WHIP + K%
# with equal weights, relative to league average. This is a rate-based proxy.
# We z-score each component and sum.

compute_fantasy_rate <- function(df) {
  df %>%
    mutate(
      z_era = -1 * scale(era)[, 1],     # lower is better
      z_whip = -1 * scale(whip)[, 1],   # lower is better
      z_k = scale(k_pct)[, 1]           # higher is better
    ) %>%
    mutate(fantasy_rate = (z_era + z_whip + z_k) / 3)
}

# Build YoY pairs with fantasy rate as target
sp_full_with_fantasy <- sp_full %>%
  group_by(season) %>%
  mutate(
    z_era = -1 * scale(era)[, 1],
    z_whip = -1 * scale(whip)[, 1],
    z_k = scale(k_pct)[, 1],
    fantasy_rate = (z_era + z_whip + z_k) / 3
  ) %>%
  ungroup()

# Pair with next-year fantasy rate
fantasy_pairs <- sp_full_with_fantasy %>%
  select(player_id, season, all_of(intersect(METRIC_CANDIDATES, names(.))),
         start_ip, tbf, pitches) %>%
  inner_join(
    sp_full_with_fantasy %>%
      select(player_id, season, fantasy_rate) %>%
      mutate(season = season - 1L) %>%
      rename(next_fantasy_rate = fantasy_rate),
    by = c("player_id", "season")
  )

if (nrow(fantasy_pairs) >= 50) {
  cat(sprintf("  Fantasy rate pairs: %d\n", nrow(fantasy_pairs)))

  # Test each metric against next-year fantasy rate
  cat("  Individual metrics → next-year fantasy rate:\n")
  for (m in intersect(available_metrics_yoy, names(fantasy_pairs))) {
    ok <- !is.na(fantasy_pairs[[m]]) & !is.na(fantasy_pairs$next_fantasy_rate)
    if (sum(ok) < 30) next
    r <- cor(fantasy_pairs[[m]][ok], fantasy_pairs$next_fantasy_rate[ok])
    aligned_r <- r * METRIC_QUALITY_SIGN[[m]]
    cat(sprintf("    %s: r=%.3f (aligned=%.3f)\n", m, r, aligned_r))
  }

  # Full model R²
  fantasy_metrics <- intersect(available_metrics_yoy, names(fantasy_pairs))
  fantasy_metrics <- fantasy_metrics[
    sapply(fantasy_metrics, function(m) sum(!is.na(fantasy_pairs[[m]])) >= 50)
  ]
  full_r2 <- ridge_cv_r2(fantasy_pairs, fantasy_metrics, "next_fantasy_rate")
  cat(sprintf("\n  Full model → next-year fantasy rate: CV R²=%.4f\n", full_r2))
} else {
  cat("  Insufficient fantasy rate pairs for analysis\n")
}

# ===========================================================================
# SECTION 12: SUMMARY & RECOMMENDATIONS
# ===========================================================================

cat("\n\n")
cat("=" |> rep(70) |> paste(collapse = ""), "\n")
cat("SUMMARY & KEY OUTPUTS\n")
cat("=" |> rep(70) |> paste(collapse = ""), "\n")

cat("\nOutput files saved to: ", OUTPUT_DIR, "\n")
cat("  - individual_metric_correlations.csv\n")
cat("  - combinatorial_subset_results.csv\n")
cat("  - forward_stepwise_results.csv\n")
cat("  - forward_stepwise_1h2h.csv\n")
cat("  - ridge_optimal_weights.csv\n")
cat("  - ridge_optimal_weights_1h2h.csv\n")
cat("  - tier_weight_comparison.csv\n")
cat("  - metric_intercorrelation_matrix.csv\n")

cat("\n\nKey questions answered:\n")
cat("  1. SwStr% + Whiff% together? → See Section 9\n")
cat("  2. Best metric subsets? → See Section 4 (stepwise + combinatorial)\n")
cat("  3. What does SP Skillz predict best? → Compare CV R² across targets\n")
cat("  4. Z-Contact analysis → See Section 8\n")
cat("  5. IP tier paradigm justified? → See Section 6\n")
cat("  6. Optimal weights? → See Section 5 (ridge)\n")

cat("\n\nDone! Review outputs and discuss results.\n")
