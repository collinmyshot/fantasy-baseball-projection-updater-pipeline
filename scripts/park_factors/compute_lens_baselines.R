#!/usr/bin/env Rscript
# League baselines for each iPF lens, used to express park effects as a
# percentage of league average rather than as a z-score.
#
# The leaderboard's default index is a z-score (100 + 10 * z over the 30
# current parks), which answers "how unusual is this park." The alternative
# scale answers "how much does this park change the rate," which is what
# Savant publishes and what most readers assume an index means. Converting
# needs the league rate each effect sits on top of, and those rates are not
# recoverable from the published tables: the component models are fit on
# residuals, so their own pf_index columns divide by a mean residual near zero
# and produce nonsense (BACON reads 861 rather than 108).
#
# Baselines computed here, on exactly the rows each model was fit on:
#   overall  mean xwOBA on contact          (the main model's expectation)
#   bacon    league hits per batted ball
#   hr       league home runs per batted ball
#   carry    mean fly ball and liner distance, feet
#   k        league strikeouts per plate appearance
#
# Output: data/processed/park_factors/lens_baselines.csv
# Run after build_park_factors.R and before build_park_factor_clean_2026.R.

source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  bbe_input  = list(flag = "--bbe-input", default = file.path("data", "raw", "statcast_bbe_store.csv")),
  pa_input   = list(flag = "--pa-input",  default = file.path("data", "raw", "statcast_pa_store.csv")),
  pf_dir     = list(flag = "--pf-dir",    default = file.path("data", "processed", "park_factors")),
  max_date   = list(flag = "--max-date",  default = ""),
  min_season = list(flag = "--min-season", default = 2015, type = "numeric"),
  exclude_seasons = list(flag = "--exclude-seasons", default = "2020"),
  out_csv    = list(flag = "--out",       default = file.path("data", "processed", "park_factors", "lens_baselines.csv"))
))

max_date <- suppressWarnings(as.Date(as.character(parsed$max_date)))
if (is.na(max_date)) {
  meta_path <- file.path(parsed$pf_dir, "run_metadata.csv")
  if (file.exists(meta_path)) {
    m <- utils::read.csv(meta_path, stringsAsFactors = FALSE)
    v <- m$value[m$key == "max_date"]
    if (length(v) > 0 && nzchar(v[[1]])) max_date <- as.Date(v[[1]])
  }
}
min_season <- as.integer(parsed$min_season)
excl <- parse_int_vec(as.character(parsed$exclude_seasons))

apply_window <- function(d) {
  d$game_date <- as.Date(d$game_date)
  d <- d[!is.na(d$game_date), ]
  if (!is.na(max_date)) d <- d[d$game_date <= max_date, ]
  d$season <- as.integer(format(d$game_date, "%Y"))
  d[d$season >= min_season & !d$season %in% excl, ]
}

message("Reading BBE store: ", parsed$bbe_input)
b <- utils::read.csv(parsed$bbe_input, stringsAsFactors = FALSE, check.names = FALSE)
b <- unique(b)
b <- apply_window(b)

# Match the model's own filters: a tracked batted ball with launch metrics and
# a real outcome. Tracked fouls carry no event and never reach the fit.
b <- b[!is.na(b$launch_speed) & !is.na(b$launch_angle), ]
b$ev <- tolower(trimws(ifelse(is.na(b$events), "", b$events)))
b <- b[nzchar(b$ev), ]
b <- b[is.na(b$game_type) | b$game_type == "R", ]

xw <- suppressWarnings(as.numeric(b$estimated_woba_using_speedangle))
overall_baseline <- mean(xw[is.finite(xw)])

hits <- c("single", "double", "triple", "home_run")
bacon_baseline <- mean(b$ev %in% hits)
hr_baseline <- mean(b$ev == "home_run")

bbt <- tolower(trimws(ifelse(is.na(b$bb_type), "", b$bb_type)))
dist <- suppressWarnings(as.numeric(b$hit_distance_sc))
air <- bbt %in% c("fly_ball", "line_drive") & is.finite(dist)
carry_baseline <- mean(dist[air])

message(sprintf("  BBE rows used: %s", format(nrow(b), big.mark = ",")))

message("Reading PA store: ", parsed$pa_input)
k_baseline <- NA_real_
if (file.exists(parsed$pa_input)) {
  p <- utils::read.csv(parsed$pa_input, stringsAsFactors = FALSE, check.names = FALSE)
  if (all(c("game_pk", "at_bat_number") %in% names(p))) {
    p <- p[!duplicated(paste(p$game_pk, p$at_bat_number)), ]
  }
  p <- apply_window(p)
  p$ev <- tolower(trimws(ifelse(is.na(p$events), "", p$events)))
  p <- p[is.na(p$game_type) | p$game_type == "R", ]
  NON_BATTER <- c("truncated_pa", "game_advisory", "caught_stealing_2b", "caught_stealing_3b",
                  "caught_stealing_home", "pickoff_1b", "pickoff_2b", "pickoff_3b",
                  "pickoff_caught_stealing_2b", "pickoff_caught_stealing_3b",
                  "pickoff_caught_stealing_home", "stolen_base_2b", "stolen_base_3b",
                  "stolen_base_home", "other_out", "runner_double_play", "wild_pitch",
                  "passed_ball", "other_advance", "ejection")
  p <- p[!p$ev %in% NON_BATTER & nzchar(p$ev), ]
  k_baseline <- mean(p$ev %in% c("strikeout", "strikeout_double_play", "strikeout_triple_play"))
  message(sprintf("  PA rows used: %s", format(nrow(p), big.mark = ",")))
} else {
  warning("PA store not found; K baseline will be NA and the K ratio scale unavailable.")
}

# Where the model published a usable ratio index, its own baseline is
# recoverable exactly (baseline = delta / (index/100 - 1)) and is preferred
# over recomputing, because it is the mean over precisely the rows the fit
# used. This works for the main model and for Carry, whose outcome is a raw
# distance. It does NOT work for BACON or HR: those models are fit on
# residuals, so the recovered denominator is a mean residual near zero and the
# published index is meaningless. Those two keep the league rates computed
# above.
recover_baseline <- function(file, delta_col, idx_col) {
  p <- file.path(parsed$pf_dir, file)
  if (!file.exists(p)) return(NA_real_)
  d <- utils::read.csv(p, stringsAsFactors = FALSE, check.names = FALSE)
  if (!all(c(delta_col, idx_col) %in% names(d))) return(NA_real_)
  delta <- suppressWarnings(as.numeric(d[[delta_col]]))
  idx <- suppressWarnings(as.numeric(d[[idx_col]]))
  est <- delta / (idx / 100 - 1)
  est <- est[is.finite(est)]
  if (length(est) == 0) return(NA_real_)
  stats::median(est)
}

overall_recovered <- recover_baseline("park_factors_overall.csv",
                                      "delta_woba_over_xwoba_overall", "pf_index_overall")
carry_recovered <- recover_baseline("park_factors_distance_overall.csv",
                                    "delta_overall", "pf_index_overall")
if (is.finite(overall_recovered)) {
  message(sprintf("Overall baseline: using model value %.5f (recomputed %.5f)",
                  overall_recovered, overall_baseline))
  overall_baseline <- overall_recovered
}
if (is.finite(carry_recovered)) {
  message(sprintf("Carry baseline: using model value %.3f (recomputed %.3f)",
                  carry_recovered, carry_baseline))
  carry_baseline <- carry_recovered
}

out <- data.frame(
  lens = c("overall", "bacon", "hr", "carry", "k"),
  baseline = c(overall_baseline, bacon_baseline, hr_baseline, carry_baseline, k_baseline),
  units = c("xwOBA on contact", "hits per BBE", "HR per BBE", "feet", "K per PA"),
  source = c(
    if (is.finite(overall_recovered)) "model" else "recomputed",
    "recomputed", "recomputed",
    if (is.finite(carry_recovered)) "model" else "recomputed",
    "recomputed"
  ),
  data_through = rep(ifelse(is.na(max_date), "", format(max_date)), 5),
  stringsAsFactors = FALSE
)
utils::write.csv(out, parsed$out_csv, row.names = FALSE, na = "")

message("\nLens baselines:")
for (i in seq_len(nrow(out))) {
  message(sprintf("  %-8s %10.5f  (%s)", out$lens[i], out$baseline[i], out$units[i]))
}
message("\nWrote: ", parsed$out_csv)
