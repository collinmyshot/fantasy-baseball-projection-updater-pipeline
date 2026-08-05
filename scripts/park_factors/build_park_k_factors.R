#!/usr/bin/env Rscript
# Park K% model: a PA-level sibling of the iPF contact model.
#
# iPF measures contact only (its store excludes strikeouts by construction),
# so this model estimates the park's effect on strikeouts per plate appearance
# from the PA store built by scripts/park_factors/fetch_statcast_pa.R. It is
# NOT part of the iPF Overall factor; its park term is surfaced as a separate
# K% display lens.
#
# Design mirrors iPF where the structure transfers:
#   outcome    is_k (0/1 per PA), linear probability model via lmer
#   REs        park_era_id, park_era_half_id, park_era_hand_id,
#              batter_season_id, pitcher_season_id,
#              fielding_team_season_id (absorbs catcher framing),
#              batting_team_season_id
#   FEs        month_grp (6 seasonal buckets, as in the August 2026 iPF),
#              hand (league batter-side level), matchup_same (platoon),
#              drag_c + drag_missing (falsification: ball carry should not
#              predict strikeouts)
# There is no defense covariate (OAA range is irrelevant to K) and no
# quality weight (every PA counts once). Park eras are the same manual
# park_era_events.csv the iPF build uses, which doubles as a falsification
# check: wall-distance era events should not move K%.
#
# A BB sidecar (unintentional walks per PA, same formula) runs as a second
# falsification target: park physics should produce approximately zero BB
# effect.
#
# The PA store keeps every events-carrying row; this script classifies the
# observed event taxonomy into batter PA outcomes (the modeling denominator)
# vs baserunning/administrative PA truncations (excluded), and writes the
# full mapping with counts to k_event_taxonomy.csv. Unrecognized event values
# are excluded and reported loudly rather than silently classified.

source(file.path("R", "utils.R"))
source(file.path("R", "park_factors.R"))

parsed <- parse_cli_args(list(
  pa_input           = list(flag = "--pa-input",       default = file.path("data", "raw", "statcast_pa_store.csv")),
  park_events_csv    = list(flag = "--park-events",    default = file.path("data", "manual", "park_era_events.csv")),
  schedule_cache_csv = list(flag = "--schedule-cache", default = file.path("data", "raw", "mlb_schedule_venues.csv")),
  drag_input         = list(flag = "--drag-input",     default = file.path("data", "processed", "drag_daily.csv")),
  output_dir         = list(flag = "--output-dir",     default = file.path("data", "processed", "park_factors")),
  min_season         = list(flag = "--min-season",     default = 2015, type = "numeric"),
  exclude_seasons    = list(flag = "--exclude-seasons", default = "2020"),
  max_date           = list(flag = "--max-date",       default = ""),
  glmer_seasons      = list(flag = "--glmer-seasons",  default = "")
))

pa_input           <- parsed$pa_input
park_events_csv    <- parsed$park_events_csv
schedule_cache_csv <- parsed$schedule_cache_csv
drag_input         <- parsed$drag_input
output_dir         <- parsed$output_dir
min_season         <- as.integer(parsed$min_season)
exclude_seasons    <- parse_int_vec(as.character(parsed$exclude_seasons))
max_date           <- suppressWarnings(as.Date(as.character(parsed$max_date)))
glmer_seasons      <- parse_int_vec(as.character(parsed$glmer_seasons))

if (!file.exists(pa_input)) {
  stop(sprintf("PA input file does not exist: %s (run scripts/park_factors/fetch_statcast_pa.R first)", pa_input))
}
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
}
if (!requireNamespace("lme4", quietly = TRUE)) {
  stop("Package 'lme4' is required.")
}

# ── Event taxonomy ────────────────────────────────────────────────────────────
# Batter PA outcomes: the modeling denominator. Everything observed in the
# store that is not listed here is excluded from the denominator; events not in
# either list are excluded AND reported as unrecognized.
K_EVENTS <- c("strikeout", "strikeout_double_play", "strikeout_triple_play")
BB_EVENTS <- c("walk")
IBB_EVENTS <- c("intent_walk")
OTHER_BATTER_OUTCOME_EVENTS <- c(
  "single", "double", "triple", "home_run",
  "field_out", "force_out", "grounded_into_double_play", "double_play",
  "triple_play", "sac_fly", "sac_fly_double_play", "sac_bunt",
  "sac_bunt_double_play", "field_error", "fielders_choice",
  "fielders_choice_out", "hit_by_pitch", "catcher_interf"
)
BATTER_OUTCOME_EVENTS <- c(K_EVENTS, BB_EVENTS, IBB_EVENTS, OTHER_BATTER_OUTCOME_EVENTS)

# PA truncations that are not batter outcomes: the batter's PA either does not
# count (inning-ending baserunning outs, truncated_pa) or the row is
# administrative.
NON_BATTER_EVENTS <- c(
  "truncated_pa",
  "caught_stealing_2b", "caught_stealing_3b", "caught_stealing_home",
  "pickoff_1b", "pickoff_2b", "pickoff_3b",
  "pickoff_caught_stealing_2b", "pickoff_caught_stealing_3b",
  "pickoff_caught_stealing_home",
  "stolen_base_2b", "stolen_base_3b", "stolen_base_home",
  "other_out", "runner_double_play", "wild_pitch", "passed_ball",
  "other_advance", "game_advisory", "ejection"
)

classify_events <- function(events_lc) {
  status <- rep("unrecognized_excluded", length(events_lc))
  status[events_lc %in% K_EVENTS] <- "k"
  status[events_lc %in% BB_EVENTS] <- "bb_unintentional"
  status[events_lc %in% IBB_EVENTS] <- "bb_intentional"
  status[events_lc %in% OTHER_BATTER_OUTCOME_EVENTS] <- "other_batter_outcome"
  status[events_lc %in% NON_BATTER_EVENTS] <- "excluded_non_batter"
  status
}

# ── Load and prepare ──────────────────────────────────────────────────────────
message("Reading PA input: ", pa_input)
pa <- utils::read.csv(pa_input, stringsAsFactors = FALSE, check.names = FALSE)
message(sprintf("  %s rows in store.", nrow(pa)))

# Deduplicate on plate-appearance IDENTITY (game_pk + at_bat_number), never on
# the whole row: two different PAs by the same batter against the same pitcher
# in the same game with the same outcome are byte-identical apart from
# at_bat_number, so a whole-row unique() deletes real plate appearances (14% of
# them in a 2015-2016 sample). A duplicate here can only come from the same
# date window being fetched twice, which is what this guards against.
if (!all(c("game_pk", "at_bat_number") %in% names(pa))) {
  stop("PA store lacks game_pk/at_bat_number; re-fetch with the current scripts/park_factors/fetch_statcast_pa.R (identity columns were added 2026-08-03).")
}
n_before <- nrow(pa)
pa_key <- paste(pa$game_pk, pa$at_bat_number)
pa <- pa[!duplicated(pa_key), , drop = FALSE]
n_dropped <- n_before - nrow(pa)
message(sprintf("  Deduplication on game_pk+at_bat_number: %d -> %d rows (removed %d)", n_before, nrow(pa), n_dropped))
if (n_dropped > 0.02 * n_before) {
  warning(sprintf(
    "Dropped %.1f%% of PA rows as duplicates. Expected ~0%% from a clean fetch; check for overlapping chunk windows in the PA store.",
    100 * n_dropped / n_before
  ))
}

pa$game_date <- as.Date(pa$game_date)
pa <- pa[!is.na(pa$game_date), ]
pa$season <- as.integer(format(pa$game_date, "%Y"))
pa <- pa[pa$season >= min_season & !pa$season %in% exclude_seasons, ]
if (!is.na(max_date)) {
  pa <- pa[pa$game_date <= max_date, ]
}
pa <- pa[is.na(pa$game_type) | pa$game_type == "R", ]
message(sprintf("  %s rows after season/date filters (%s..%s).", nrow(pa), min(pa$game_date), max(pa$game_date)))

pa$events_lc <- tolower(trimws(pa$events))
pa$event_class <- classify_events(pa$events_lc)

taxonomy <- stats::aggregate(
  list(n = rep(1L, nrow(pa))),
  by = list(events = pa$events_lc, status = pa$event_class),
  FUN = sum
)
taxonomy <- taxonomy[order(-taxonomy$n), ]
taxonomy$in_denominator <- taxonomy$status %in% c("k", "bb_unintentional", "bb_intentional", "other_batter_outcome")
utils::write.csv(taxonomy, file.path(output_dir, "k_event_taxonomy.csv"), row.names = FALSE, na = "")
message("Event taxonomy written (k_event_taxonomy.csv):")
for (i in seq_len(nrow(taxonomy))) {
  message(sprintf("  %-28s %-22s %9d", taxonomy$events[i], taxonomy$status[i], taxonomy$n[i]))
}

unrec <- taxonomy[taxonomy$status == "unrecognized_excluded", , drop = FALSE]
if (nrow(unrec) > 0) {
  warning(sprintf(
    "Unrecognized event values excluded from the PA denominator: %s. Review and extend the taxonomy lists.",
    paste(sprintf("%s (n=%d)", unrec$events, unrec$n), collapse = ", ")
  ))
}

pa <- pa[pa$event_class %in% c("k", "bb_unintentional", "bb_intentional", "other_batter_outcome"), ]
pa$is_k <- as.integer(pa$event_class == "k")
pa$is_bb <- as.integer(pa$event_class == "bb_unintentional")
message(sprintf(
  "  Modeling denominator: %s PAs | K rate %.4f | UIBB rate %.4f",
  nrow(pa), mean(pa$is_k), mean(pa$is_bb)
))

# ── Park era assignment (same machinery as the iPF build) ─────────────────────
schedule_data <- data.frame()
if (nzchar(schedule_cache_csv) && file.exists(schedule_cache_csv)) {
  schedule_data <- utils::read.csv(schedule_cache_csv, stringsAsFactors = FALSE, check.names = FALSE)
  message(sprintf("Loaded schedule venue cache: %s rows.", nrow(schedule_data)))
} else {
  warning(sprintf("Schedule cache not found (%s); venue ids fall back to home_team.", schedule_cache_csv))
}

pa$home_team <- normalize_team_abbrev_pf(pa$home_team)
pa$away_team <- normalize_team_abbrev_pf(pa$away_team)

if (is.data.frame(schedule_data) && nrow(schedule_data) > 0) {
  pa <- merge_schedule_venues(pa, schedule_data)
}
if (!"venue_id" %in% names(pa)) pa$venue_id <- NA_real_
if (!"venue_name" %in% names(pa)) pa$venue_name <- ""
pa$venue_id <- suppressWarnings(as.numeric(pa$venue_id))
pa$venue_name <- as.character(pa$venue_name)
pa$venue_name[is.na(pa$venue_name)] <- ""
venue_cov <- mean(!is.na(pa$venue_id))
message(sprintf("Venue id coverage: %.2f%%", 100 * venue_cov))

pa$base_park_id <- ifelse(
  !is.na(pa$venue_id),
  paste0("venue_", pa$venue_id),
  paste0("home_", pa$home_team)
)
pa$base_park_id <- slugify_text(pa$base_park_id)

park_events <- load_park_events(park_events_csv)
pa <- apply_park_events(pa, park_events)

# ── Seasonal / identity structure ─────────────────────────────────────────────
pa$month <- as.integer(format(pa$game_date, "%m"))
pa$half <- ifelse(pa$month >= 3 & pa$month <= 6, "1H", ifelse(pa$month >= 7 & pa$month <= 10, "2H", NA_character_))
pa <- pa[!is.na(pa$half), ]

month_bucket <- c(
  "3" = "mar_apr", "4" = "mar_apr", "5" = "may", "6" = "jun",
  "7" = "jul", "8" = "aug", "9" = "sep_oct", "10" = "sep_oct"
)
pa$month_grp <- factor(
  month_bucket[as.character(pa$month)],
  levels = c("mar_apr", "may", "jun", "jul", "aug", "sep_oct")
)

inning_half <- tolower(trimws(pa$inning_topbot))
is_top <- grepl("top", inning_half)
pa$batting_team <- ifelse(is_top, pa$away_team, pa$home_team)
pa$fielding_team <- ifelse(is_top, pa$home_team, pa$away_team)

pa$batter_season_id <- paste0("b", pa$batter, "_", pa$season)
pa$pitcher_season_id <- paste0("p", pa$pitcher, "_", pa$season)
pa$fielding_team_season_id <- paste0(pa$fielding_team, "_", pa$season)
pa$batting_team_season_id <- paste0(pa$batting_team, "_", pa$season)
pa$park_era_half_id <- paste0(pa$park_era_id, "__", pa$half)

pa$hand <- ifelse(pa$stand %in% c("L", "R"), pa$stand, "U")
pa$park_era_hand_id <- paste0(pa$park_era_id, "__", pa$hand)
pa$matchup_same <- as.integer(pa$stand == pa$p_throws)

# ── Drag join (falsification covariate) ───────────────────────────────────────
pa$league_cd <- NA_real_
if (nzchar(drag_input) && file.exists(drag_input)) {
  drag_data <- utils::read.csv(drag_input, stringsAsFactors = FALSE, check.names = FALSE)
  drag_key <- data.frame(
    game_date = as.Date(as.character(first_present_column(drag_data, c("game_date", "date")))),
    league_cd_join = suppressWarnings(as.numeric(first_present_column(drag_data, c("mean_cd", "league_cd", "drag")))),
    stringsAsFactors = FALSE
  )
  drag_key <- drag_key[!is.na(drag_key$game_date) & is.finite(drag_key$league_cd_join), ]
  if (nrow(drag_key) > 0) {
    pa <- merge(pa, drag_key, by = "game_date", all.x = TRUE)
    pa$league_cd <- pa$league_cd_join
    pa$league_cd_join <- NULL
  }
} else {
  warning(sprintf("Drag input not found (%s); K model runs without the drag falsification covariate.", drag_input))
}

center_with_indicator <- function(x) {
  ok <- is.finite(x)
  ctr <- mean(x[ok])
  list(centered = ifelse(ok, x - ctr, 0), missing = ifelse(ok, 0, 1))
}
drag_ci <- center_with_indicator(pa$league_cd)
pa$drag_c <- drag_ci$centered
pa$drag_missing <- drag_ci$missing
message(sprintf("Drag coverage: %.1f%%", 100 * mean(pa$drag_missing == 0)))

# ── Fit ───────────────────────────────────────────────────────────────────────
fit_pa_lpm <- function(d, outcome_col) {
  fixed_terms <- c("month_grp", "hand", "matchup_same")
  for (nm in c("drag_c", "drag_missing")) {
    vals <- d[[nm]][is.finite(d[[nm]])]
    if (length(unique(vals)) >= 2) {
      fixed_terms <- c(fixed_terms, nm)
    }
  }
  rhs <- c(
    fixed_terms,
    "(1 | park_era_id)",
    "(1 | park_era_half_id)",
    "(1 | park_era_hand_id)",
    "(1 | batter_season_id)",
    "(1 | pitcher_season_id)",
    "(1 | fielding_team_season_id)",
    "(1 | batting_team_season_id)"
  )
  form <- stats::as.formula(paste(outcome_col, "~", paste(rhs, collapse = " + ")))
  message("Fitting: ", deparse(form))
  message("Rows: ", nrow(d))
  fit <- lme4::lmer(
    formula = form,
    data = d,
    REML = TRUE,
    control = lme4::lmerControl(optimizer = "bobyqa", calc.derivs = FALSE)
  )
  list(fit = fit, formula = form, baseline = mean(d[[outcome_col]]), outcome_col = outcome_col)
}

era_meta <- function(d) {
  agg_n <- stats::aggregate(list(n_pa = rep(1L, nrow(d))), by = list(park_era_id = d$park_era_id), FUN = sum)
  agg_k <- stats::aggregate(list(n_k = d$is_k), by = list(park_era_id = d$park_era_id), FUN = sum)
  agg_rate <- stats::aggregate(list(obs_k_rate = d$is_k), by = list(park_era_id = d$park_era_id), FUN = mean)
  agg_years <- stats::aggregate(
    list(years_used = d$season),
    by = list(park_era_id = d$park_era_id),
    FUN = function(s) {
      rng <- range(s)
      if (rng[1] == rng[2]) as.character(rng[1]) else paste0(rng[1], "-", rng[2])
    }
  )
  agg_team <- stats::aggregate(
    list(home_team = d$home_team, venue_name = d$venue_name),
    by = list(park_era_id = d$park_era_id),
    FUN = function(x) names(sort(table(x), decreasing = TRUE))[1]
  )
  out <- Reduce(function(a, b) merge(a, b, by = "park_era_id"), list(agg_n, agg_k, agg_rate, agg_years, agg_team))
  out
}

extract_park_tables <- function(model, d, prefix) {
  fit <- model$fit
  baseline <- model$baseline

  park_re <- extract_random_effects_with_se(fit, "park_era_id")
  names(park_re) <- c("park_era_id", "park_effect", "park_se")

  meta <- era_meta(d)
  overall <- merge(meta, park_re, by = "park_era_id", all.x = TRUE)
  overall$park_effect[is.na(overall$park_effect)] <- 0
  overall$idx_100 <- 100 * (1 + overall$park_effect / baseline)
  overall$obs_idx_100 <- 100 * (overall$obs_k_rate / baseline)
  overall <- overall[order(-overall$idx_100), ]

  half_re <- extract_random_effects_with_se(fit, "park_era_half_id")
  names(half_re) <- c("park_era_half_id", "half_effect", "half_se")
  halves <- unique(d[, c("park_era_id", "half", "park_era_half_id")])
  halves <- merge(halves, park_re, by = "park_era_id", all.x = TRUE)
  halves <- merge(halves, half_re, by = "park_era_half_id", all.x = TRUE)
  halves$park_effect[is.na(halves$park_effect)] <- 0
  halves$half_effect[is.na(halves$half_effect)] <- 0
  halves$delta <- halves$park_effect + halves$half_effect
  halves$idx_100 <- 100 * (1 + halves$delta / baseline)
  halves <- halves[order(halves$park_era_id, halves$half), ]

  hand_re <- extract_random_effects_with_se(fit, "park_era_hand_id")
  names(hand_re) <- c("park_era_hand_id", "hand_effect", "hand_se")
  hands <- unique(d[d$hand %in% c("L", "R"), c("park_era_id", "hand", "park_era_hand_id")])
  hands <- merge(hands, park_re, by = "park_era_id", all.x = TRUE)
  hands <- merge(hands, hand_re, by = "park_era_hand_id", all.x = TRUE)
  hands$park_effect[is.na(hands$park_effect)] <- 0
  hands$hand_effect[is.na(hands$hand_effect)] <- 0
  hands$delta <- hands$park_effect + hands$hand_effect
  hands$idx_100 <- 100 * (1 + hands$delta / baseline)
  hands <- hands[order(hands$park_era_id, hands$hand), ]

  # Standard errors matter here specifically: the drag term is a falsification
  # check, and "indistinguishable from zero" is a claim that needs an SE behind
  # it rather than an eyeball on the point estimate.
  fe <- lme4::fixef(fit)
  se <- tryCatch(sqrt(diag(as.matrix(stats::vcov(fit)))), error = function(e) rep(NA_real_, length(fe)))
  fixef_df <- data.frame(
    model = prefix,
    term = names(fe),
    estimate = as.numeric(fe),
    std_error = as.numeric(se[names(fe)]),
    stringsAsFactors = FALSE
  )
  fixef_df$t_value <- fixef_df$estimate / fixef_df$std_error

  list(overall = overall, halves = halves, hands = hands, fixef = fixef_df, baseline = baseline)
}

message("\n=== K per PA model ===")
k_model <- fit_pa_lpm(pa, "is_k")
k_tables <- extract_park_tables(k_model, pa, "k_per_pa")

message("\n=== BB (unintentional) per PA sidecar ===")
bb_model <- fit_pa_lpm(pa, "is_bb")
bb_tables <- extract_park_tables(bb_model, pa, "bb_per_pa")

# ── Outputs ───────────────────────────────────────────────────────────────────
utils::write.csv(k_tables$overall, file.path(output_dir, "park_factors_k_overall.csv"), row.names = FALSE, na = "")
utils::write.csv(k_tables$halves, file.path(output_dir, "park_factors_k_by_half.csv"), row.names = FALSE, na = "")
utils::write.csv(k_tables$hands, file.path(output_dir, "park_factors_k_by_hand.csv"), row.names = FALSE, na = "")
utils::write.csv(bb_tables$overall, file.path(output_dir, "park_factors_bb_overall.csv"), row.names = FALSE, na = "")
fixef_all <- rbind(k_tables$fixef, bb_tables$fixef)
utils::write.csv(fixef_all, file.path(output_dir, "k_model_fixed_effects.csv"), row.names = FALSE, na = "")

run_meta <- data.frame(
  key = c(
    "run_timestamp_utc", "pa_input", "park_events_csv", "schedule_cache_csv",
    "drag_input", "max_date", "min_season", "exclude_seasons",
    "rows_modeled", "seasons_modeled", "venue_id_coverage",
    "drag_coverage_share", "baseline_k_rate", "baseline_bb_rate",
    "k_park_effect_sd", "bb_park_effect_sd"
  ),
  value = c(
    format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    pa_input, park_events_csv, schedule_cache_csv, drag_input,
    ifelse(is.na(max_date), "", format(max_date)),
    as.character(min_season),
    paste(exclude_seasons, collapse = ","),
    as.character(nrow(pa)),
    paste(sort(unique(pa$season)), collapse = ","),
    sprintf("%.4f", venue_cov),
    sprintf("%.4f", mean(pa$drag_missing == 0)),
    sprintf("%.6f", k_model$baseline),
    sprintf("%.6f", bb_model$baseline),
    sprintf("%.6f", stats::sd(k_tables$overall$park_effect)),
    sprintf("%.6f", stats::sd(bb_tables$overall$park_effect))
  ),
  stringsAsFactors = FALSE
)
utils::write.csv(run_meta, file.path(output_dir, "k_model_run_metadata.csv"), row.names = FALSE, na = "")

message("\nTop and bottom K park eras (idx over league baseline):")
show <- k_tables$overall[, c("park_era_id", "home_team", "years_used", "n_pa", "idx_100", "obs_idx_100")]
print(utils::head(show, 8))
print(utils::tail(show, 8))
message("\nFixed effects (K model):")
print(k_tables$fixef)

# ── Optional glmer spot-check on a season subset ──────────────────────────────
if (length(glmer_seasons) > 0) {
  message(sprintf("\n=== glmer binomial spot-check on seasons %s ===", paste(glmer_seasons, collapse = ",")))
  sub <- pa[pa$season %in% glmer_seasons, ]
  message(sprintf("Subset rows: %s", nrow(sub)))

  lpm_sub <- fit_pa_lpm(sub, "is_k")
  lpm_re <- extract_random_effects_with_se(lpm_sub$fit, "park_era_id")
  names(lpm_re) <- c("park_era_id", "lpm_effect", "lpm_se")

  glmer_form <- stats::update(lpm_sub$formula, . ~ .)
  glmer_fit <- lme4::glmer(
    formula = glmer_form,
    data = sub,
    family = stats::binomial(),
    nAGQ = 0,
    control = lme4::glmerControl(optimizer = "bobyqa", calc.derivs = FALSE)
  )
  glmer_re <- extract_random_effects_with_se(glmer_fit, "park_era_id")
  names(glmer_re) <- c("park_era_id", "glmer_logit_effect", "glmer_se")

  chk <- merge(lpm_re, glmer_re, by = "park_era_id")
  # Convert the glmer park effect from logit to probability scale at the
  # subset base rate for a like-for-like comparison.
  p0 <- mean(sub$is_k)
  chk$glmer_prob_effect <- stats::plogis(stats::qlogis(p0) + chk$glmer_logit_effect) - p0
  r2 <- stats::cor(chk$lpm_effect, chk$glmer_prob_effect)^2
  message(sprintf("LPM vs glmer park effects: R^2 = %.4f over %d park eras", r2, nrow(chk)))
  chk$r_squared_lpm_vs_glmer <- r2
  utils::write.csv(chk, file.path(output_dir, "k_glmer_check.csv"), row.names = FALSE, na = "")
}

message("\nDone. Outputs in ", output_dir)
