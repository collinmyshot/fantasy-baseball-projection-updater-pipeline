#!/usr/bin/env Rscript
source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  bbe_input          = list(flag = "--bbe-input",          default = ""),
  defense_input      = list(flag = "--defense-input",      default = ""),
  drag_input         = list(flag = "--drag-input",         default = file.path("data", "processed", "drag_daily.csv")),
  park_events_csv    = list(flag = "--park-events",        default = file.path("data", "manual", "park_era_events.csv")),
  schedule_cache_csv = list(flag = "--schedule-cache",     default = file.path("data", "raw", "mlb_schedule_venues.csv")),
  output_dir         = list(flag = "--output-dir",         default = file.path("data", "processed", "park_factors")),
  min_season         = list(flag = "--min-season",         default = 2015, type = "numeric"),
  exclude_seasons    = list(flag = "--exclude-seasons",    default = "2020"),
  max_date           = list(flag = "--max-date",           default = ""),
  train_window       = list(flag = "--train-window",       default = 3, type = "numeric"),
  min_train_seasons  = list(flag = "--min-train-seasons",  default = 3, type = "numeric"),
  max_rows           = list(flag = "--max-rows",           default = 0, type = "numeric"),
  seed               = list(flag = "--seed",               default = 42, type = "numeric"),
  measurement_era_mode = list(flag = "--measurement-era",  default = "off"),
  skip_schedule_fetch  = list(flag = "--skip-schedule-fetch", default = FALSE, type = "boolean")
))

bbe_input          <- parsed$bbe_input
defense_input      <- parsed$defense_input
drag_input         <- parsed$drag_input
park_events_csv    <- parsed$park_events_csv
schedule_cache_csv <- parsed$schedule_cache_csv
output_dir         <- parsed$output_dir
min_season         <- as.integer(parsed$min_season)
exclude_seasons    <- parse_int_vec(as.character(parsed$exclude_seasons))
max_date           <- suppressWarnings(as.Date(as.character(parsed$max_date)))
train_window       <- as.integer(parsed$train_window)
min_train_seasons  <- as.integer(parsed$min_train_seasons)
max_rows           <- as.integer(parsed$max_rows)
seed               <- as.integer(parsed$seed)
measurement_era_mode <- tolower(trimws(parsed$measurement_era_mode))
skip_schedule_fetch  <- parsed$skip_schedule_fetch

if (!nzchar(bbe_input)) {
  stop("You must provide --bbe-input path to a Statcast BBE CSV.")
}
if (!file.exists(bbe_input)) {
  stop(sprintf("BBE input file does not exist: %s", bbe_input))
}
if (!measurement_era_mode %in% c("auto", "on", "off")) {
  stop("--measurement-era must be one of: auto, on, off")
}

if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
}

source(file.path("R", "park_factors.R"))

message("Reading BBE input: ", bbe_input)
bbe_raw <- utils::read.csv(bbe_input, stringsAsFactors = FALSE, check.names = FALSE)
if (nrow(bbe_raw) == 0) {
  stop("BBE input is empty.")
}

# Deduplicate: legacy chunk fetches double-fetched boundary days (game_date_gt
# was inclusive, not exclusive as assumed), creating ~20-25% exact duplicates.
# Fixed in fetch_statcast_bbe.R 2026-07-01; kept as a cheap safety net.
n_before <- nrow(bbe_raw)
bbe_raw <- unique(bbe_raw)
message(sprintf("  Deduplication: %d -> %d rows (removed %d duplicates)",
                n_before, nrow(bbe_raw), n_before - nrow(bbe_raw)))

if (is.finite(max_rows) && max_rows > 0 && nrow(bbe_raw) > max_rows) {
  set.seed(seed)
  keep <- sort(sample.int(nrow(bbe_raw), max_rows))
  bbe_raw <- bbe_raw[keep, ]
  message(sprintf("Sampled %s rows for modeling.", nrow(bbe_raw)))
}

preview <- standardize_bbe_columns(bbe_raw)
preview <- preview[!is.na(preview$game_date), ]
preview$season <- as.integer(format(preview$game_date, "%Y"))
preview <- preview[preview$season >= min_season & !preview$season %in% exclude_seasons, ]

if (nrow(preview) == 0) {
  stop("No rows remain after season filtering.")
}

schedule_data <- data.frame()
if (nzchar(schedule_cache_csv) && file.exists(schedule_cache_csv)) {
  schedule_data <- utils::read.csv(schedule_cache_csv, stringsAsFactors = FALSE, check.names = FALSE)
}

missing_venue <- !"venue_id" %in% names(preview) || any(is.na(preview$venue_id))
seasons_needed <- sort(unique(preview$season))

if (!skip_schedule_fetch && missing_venue) {
  cached_seasons <- integer(0)
  if (is.data.frame(schedule_data) && nrow(schedule_data) > 0 && "season" %in% names(schedule_data)) {
    cached_seasons <- sort(unique(as.integer(schedule_data$season)))
    cached_seasons <- cached_seasons[!is.na(cached_seasons)]
  }
  fetch_seasons <- setdiff(seasons_needed, cached_seasons)

  if (length(fetch_seasons) > 0) {
    message("Fetching schedule/venue data for seasons: ", paste(fetch_seasons, collapse = ", "))
    fetched <- fetch_schedule_venues(fetch_seasons, game_type = "R")
    if (nrow(fetched) > 0) {
      schedule_data <- if (nrow(schedule_data) > 0) rbind(schedule_data, fetched) else fetched
      schedule_data <- schedule_data[!duplicated(schedule_data$game_pk), ]
      rownames(schedule_data) <- NULL

      cache_dir <- dirname(schedule_cache_csv)
      if (!dir.exists(cache_dir)) {
        dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
      }
      utils::write.csv(schedule_data, schedule_cache_csv, row.names = FALSE, na = "")
      message("Updated schedule cache: ", schedule_cache_csv)
    }
  }
}

if (missing_venue) {
  if (is.data.frame(schedule_data) && nrow(schedule_data) > 0) {
    message(sprintf("Schedule venue rows available: %s", nrow(schedule_data)))
  } else {
    warning(
      "No schedule venue data available; falling back to team-based park IDs. ",
      "This can conflate same-team multi-park seasons/eras."
    )
  }
}

park_events <- load_park_events(park_events_csv)

defense_data <- data.frame()
if (nzchar(defense_input) && file.exists(defense_input)) {
  defense_raw <- utils::read.csv(defense_input, stringsAsFactors = FALSE, check.names = FALSE)
  defense_data <- build_defense_composite(defense_raw)
  message(sprintf("Loaded defense composite for %s team-seasons.", nrow(defense_data)))
}

drag_data <- data.frame()
if (nzchar(drag_input) && file.exists(drag_input)) {
  drag_data <- utils::read.csv(drag_input, stringsAsFactors = FALSE, check.names = FALSE)
  message(sprintf("Loaded daily drag series: %s rows.", nrow(drag_data)))
} else if (nzchar(drag_input)) {
  warning(sprintf("Drag input not found (%s); model runs without drag adjustment.", drag_input))
}

model_data <- prepare_bbe_model_data(
  bbe_raw = bbe_raw,
  schedule_data = schedule_data,
  park_events = park_events,
  defense_data = defense_data,
  drag_data = drag_data,
  min_season = min_season,
  exclude_seasons = exclude_seasons,
  regular_season_only = TRUE
)

if (!is.na(max_date)) {
  n_before_cap <- nrow(model_data)
  model_data <- model_data[!is.na(model_data$game_date) & model_data$game_date <= max_date, ]
  message(sprintf("Applied --max-date %s: %s -> %s rows.", format(max_date), n_before_cap, nrow(model_data)))
}

if (nrow(model_data) < 2000) {
  stop(sprintf("Insufficient modeled BBEs after filters: %s", nrow(model_data)))
}

drag_coverage <- mean(model_data$drag_missing == 0)
message(sprintf("Modeling rows: %s (drag coverage: %.1f%%)", nrow(model_data), 100 * drag_coverage))
message(sprintf("Seasons in model data: %s", paste(sort(unique(model_data$season)), collapse = ", ")))

include_measurement_era <- FALSE
if (measurement_era_mode == "auto") {
  message("Starting measurement-era comparison via rolling validation...")
  measurement_cmp <- compare_measurement_era_models(
    model_data = model_data,
    train_window = train_window,
    min_train_seasons = min_train_seasons,
    verbose = TRUE
  )
  message("Measurement-era comparison complete.")

  cmp <- measurement_cmp$summary
  use_auto <- TRUE
  if (nrow(cmp) == 2 && all(is.finite(cmp$mean_rmse_model))) {
    with_rmse <- cmp$mean_rmse_model[cmp$model == "with_measurement_era"]
    without_rmse <- cmp$mean_rmse_model[cmp$model == "without_measurement_era"]
    include_measurement_era <- with_rmse <= without_rmse
    use_auto <- FALSE
  }
  if (use_auto) {
    include_measurement_era <- FALSE
  }
} else {
  include_measurement_era <- identical(measurement_era_mode, "on")
  message(sprintf(
    "Skipping measurement-era ON/OFF comparison because --measurement-era=%s.",
    measurement_era_mode
  ))
  measurement_cmp <- list(
    summary = data.frame(
      model = ifelse(include_measurement_era, "with_measurement_era", "without_measurement_era"),
      mean_rmse_model = NA_real_,
      mean_corr_model_vs_realized = NA_real_,
      mean_calibration_slope = NA_real_,
      seasons_evaluated = 0L,
      stringsAsFactors = FALSE
    )
  )
}

utils::write.csv(
  measurement_cmp$summary,
  file.path(output_dir, "measurement_era_comparison.csv"),
  row.names = FALSE,
  na = ""
)

message(sprintf("Using measurement-era term: %s", include_measurement_era))

message("Starting main rolling validation with selected model...")
validation <- rolling_validate_park_factors(
  model_data = model_data,
  train_window = train_window,
  min_train_seasons = min_train_seasons,
  include_measurement_era = include_measurement_era,
  verbose = TRUE
)
message("Main rolling validation complete.")

message("Fitting final park-factor model on full dataset...")
final_fit <- fit_park_factor_model(
  model_data = model_data,
  include_measurement_era = include_measurement_era,
  quiet = FALSE
)
message("Final park-factor model fit complete.")

message("Extracting half/overall park-factor tables...")
park_factors_half <- extract_park_factors(final_fit, model_data)

message("Extracting batter-side park-factor tables...")
park_factors_hand <- extract_hand_park_factors(final_fit, model_data, label = "woba_over_xwoba")

park_totals <- stats::aggregate(n_bbe ~ park_era_id, park_factors_half, sum)
park_overall <- park_factors_half[!duplicated(park_factors_half$park_era_id), c(
  "park_era_id", "venue_id", "venue_name", "home_team",
  "delta_woba_over_xwoba_overall", "pf_index_overall", "overall_se"
)]
park_overall <- merge(park_overall, park_totals, by = "park_era_id", all.x = TRUE)
park_overall <- park_overall[order(park_overall$pf_index_overall, decreasing = TRUE), ]

empty_component_half <- function() {
  data.frame(
    park_era_id = character(0),
    half = character(0),
    n_bbe = integer(0),
    venue_id = numeric(0),
    venue_name = character(0),
    home_team = character(0),
    park_era_half_id = character(0),
    park_effect = numeric(0),
    park_se = numeric(0),
    half_effect = numeric(0),
    half_se = numeric(0),
    delta_overall = numeric(0),
    delta_half = numeric(0),
    pf_index_overall = numeric(0),
    pf_index_half = numeric(0),
    component = character(0),
    stringsAsFactors = FALSE
  )
}

empty_component_overall <- function() {
  data.frame(
    park_era_id = character(0),
    venue_id = numeric(0),
    venue_name = character(0),
    home_team = character(0),
    component = character(0),
    delta_overall = numeric(0),
    pf_index_overall = numeric(0),
    park_se = numeric(0),
    n_bbe = integer(0),
    stringsAsFactors = FALSE
  )
}

fit_component_or_empty <- function(
  model_data,
  include_measurement_era,
  outcome_col,
  label,
  include_contact_shape = FALSE,
  fallback_outcome = "",
  fallback_label = "",
  fallback_include_contact_shape = FALSE
) {
  outcome_used <- outcome_col
  label_used <- label
  shape_used <- include_contact_shape

  available <- outcome_col %in% names(model_data) && any(is.finite(model_data[[outcome_col]]))
  if (!isTRUE(available) && nzchar(fallback_outcome)) {
    fallback_ok <- fallback_outcome %in% names(model_data) && any(is.finite(model_data[[fallback_outcome]]))
    if (isTRUE(fallback_ok)) {
      outcome_used <- fallback_outcome
      label_used <- ifelse(nzchar(fallback_label), fallback_label, fallback_outcome)
      shape_used <- isTRUE(fallback_include_contact_shape)
      available <- TRUE
    }
  }

  if (!isTRUE(available)) {
    return(list(
      available = FALSE,
      outcome_used = NA_character_,
      label_used = NA_character_,
      half = empty_component_half(),
      overall = empty_component_overall()
    ))
  }

  fit_obj <- tryCatch(
    fit_component_model(
      model_data = model_data,
      outcome_col = outcome_used,
      include_measurement_era = include_measurement_era,
      include_contact_shape = shape_used,
      quiet = TRUE
    ),
    error = function(e) {
      warning(sprintf(
        "Component fit failed for '%s' (label '%s'): %s",
        outcome_used,
        label_used,
        conditionMessage(e)
      ))
      NULL
    }
  )

  if (is.null(fit_obj)) {
    return(list(
      available = FALSE,
      outcome_used = outcome_used,
      label_used = label_used,
      half = empty_component_half(),
      overall = empty_component_overall(),
      hand = data.frame(),
      fixef = data.frame()
    ))
  }

  fixef_tbl <- tryCatch(
    {
      fe <- lme4::fixef(fit_obj$fit)
      data.frame(
        model = label_used,
        term = names(fe),
        estimate = as.numeric(fe),
        stringsAsFactors = FALSE
      )
    },
    error = function(e) data.frame()
  )

  half_tbl <- extract_component_park_factors(fit_obj, model_data, label = label_used)
  overall_tbl <- half_tbl[!duplicated(half_tbl$park_era_id), c(
    "park_era_id", "venue_id", "venue_name", "home_team", "component",
    "delta_overall", "pf_index_overall", "park_se"
  )]
  overall_tbl <- merge(
    overall_tbl,
    stats::aggregate(n_bbe ~ park_era_id, half_tbl, sum),
    by = "park_era_id",
    all.x = TRUE
  )
  overall_tbl <- overall_tbl[order(overall_tbl$pf_index_overall, decreasing = TRUE), ]

  hand_tbl <- tryCatch(
    extract_hand_park_factors(fit_obj, model_data, label = label_used),
    error = function(e) {
      warning(sprintf("Hand extraction failed for '%s': %s", label_used, conditionMessage(e)))
      data.frame()
    }
  )

  list(
    available = TRUE,
    outcome_used = outcome_used,
    label_used = label_used,
    half = half_tbl,
    overall = overall_tbl,
    hand = hand_tbl,
    fixef = fixef_tbl
  )
}

component_specs <- list(
  list(
    key = "bacon",
    title = "BACON residual",
    outcome = "bacon_resid",
    label = "bacon_resid",
    include_contact_shape = FALSE,
    fallback_outcome = "",
    fallback_label = "",
    fallback_include_contact_shape = FALSE
  ),
  list(
    key = "hr",
    title = "HR residual",
    outcome = "hr_resid",
    label = "hr_resid",
    include_contact_shape = FALSE,
    fallback_outcome = "hr_on_contact",
    fallback_label = "hr_on_contact_pseudo",
    fallback_include_contact_shape = TRUE
  ),
  list(
    key = "xbh",
    title = "XBH residual",
    outcome = "xbh_resid",
    label = "xbh_resid",
    include_contact_shape = FALSE,
    fallback_outcome = "xbh_on_contact",
    fallback_label = "xbh_on_contact_pseudo",
    fallback_include_contact_shape = TRUE
  ),
  list(
    key = "double",
    title = "2B residual",
    outcome = "double_resid",
    label = "double_resid",
    include_contact_shape = FALSE,
    fallback_outcome = "double_on_contact",
    fallback_label = "double_on_contact_pseudo",
    fallback_include_contact_shape = TRUE
  ),
  list(
    key = "triple",
    title = "3B residual",
    outcome = "triple_resid",
    label = "triple_resid",
    include_contact_shape = FALSE,
    fallback_outcome = "triple_on_contact",
    fallback_label = "triple_on_contact_pseudo",
    fallback_include_contact_shape = TRUE
  ),
  list(
    key = "distance",
    title = "Carry (air-ball distance)",
    outcome = "hit_distance_air",
    label = "distance_air",
    # Distance is a raw outcome, not a residual: EV/LA polynomials serve as the
    # expectation, and the park effect is estimated in feet.
    include_contact_shape = TRUE,
    fallback_outcome = "",
    fallback_label = "",
    fallback_include_contact_shape = FALSE
  )
)

component_results <- list()
for (spec in component_specs) {
  message(sprintf("Fitting %s component model...", spec$title))
  component_results[[spec$key]] <- fit_component_or_empty(
    model_data = model_data,
    include_measurement_era = include_measurement_era,
    outcome_col = spec$outcome,
    label = spec$label,
    include_contact_shape = spec$include_contact_shape,
    fallback_outcome = spec$fallback_outcome,
    fallback_label = spec$fallback_label,
    fallback_include_contact_shape = spec$fallback_include_contact_shape
  )

  if (isTRUE(component_results[[spec$key]]$available)) {
    message(sprintf(
      "%s component model fit complete (outcome=%s).",
      spec$title,
      component_results[[spec$key]]$outcome_used
    ))
  } else {
    message(sprintf("%s component model skipped (no usable outcome column).", spec$title))
  }
}

park_factors_bacon_half <- component_results$bacon$half
park_factors_bacon_overall <- component_results$bacon$overall
park_factors_hr_half <- component_results$hr$half
park_factors_hr_overall <- component_results$hr$overall
park_factors_xbh_half <- component_results$xbh$half
park_factors_xbh_overall <- component_results$xbh$overall
park_factors_double_half <- component_results$double$half
park_factors_double_overall <- component_results$double$overall
park_factors_triple_half <- component_results$triple$half
park_factors_triple_overall <- component_results$triple$overall
park_factors_distance_half <- component_results$distance$half
park_factors_distance_overall <- component_results$distance$overall

component_half_all <- do.call(rbind, lapply(component_results, function(x) x$half))
component_overall_all <- do.call(rbind, lapply(component_results, function(x) x$overall))
component_hand_all <- do.call(rbind, lapply(component_results, function(x) x$hand))

team_era <- summarize_team_park_eras(model_data)
invariance <- compute_invariance_checks(model_data, park_factors_half)

# Fixed-effect estimates across all models — used to verify the drag
# adjustment (e.g. the Carry model's drag_c slope should be negative, in the
# neighborhood of Savant's "-0.01 Cd ~ +5 ft at 100 mph EV" rule of thumb).
main_fixef <- tryCatch(
  {
    fe <- lme4::fixef(final_fit$fit)
    data.frame(model = "woba_over_xwoba", term = names(fe), estimate = as.numeric(fe), stringsAsFactors = FALSE)
  },
  error = function(e) data.frame()
)
component_fixef <- do.call(rbind, lapply(component_results, function(x) x$fixef))
fixed_effects_all <- rbind(main_fixef, component_fixef)
rownames(fixed_effects_all) <- NULL

utils::write.csv(park_factors_half, file.path(output_dir, "park_factors_by_half.csv"), row.names = FALSE, na = "")
utils::write.csv(park_overall, file.path(output_dir, "park_factors_overall.csv"), row.names = FALSE, na = "")
utils::write.csv(park_factors_bacon_half, file.path(output_dir, "park_factors_bacon_by_half.csv"), row.names = FALSE, na = "")
utils::write.csv(park_factors_bacon_overall, file.path(output_dir, "park_factors_bacon_overall.csv"), row.names = FALSE, na = "")
utils::write.csv(park_factors_hr_half, file.path(output_dir, "park_factors_hr_by_half.csv"), row.names = FALSE, na = "")
utils::write.csv(park_factors_hr_overall, file.path(output_dir, "park_factors_hr_overall.csv"), row.names = FALSE, na = "")
utils::write.csv(park_factors_xbh_half, file.path(output_dir, "park_factors_xbh_by_half.csv"), row.names = FALSE, na = "")
utils::write.csv(park_factors_xbh_overall, file.path(output_dir, "park_factors_xbh_overall.csv"), row.names = FALSE, na = "")
utils::write.csv(park_factors_double_half, file.path(output_dir, "park_factors_double_by_half.csv"), row.names = FALSE, na = "")
utils::write.csv(park_factors_double_overall, file.path(output_dir, "park_factors_double_overall.csv"), row.names = FALSE, na = "")
utils::write.csv(park_factors_triple_half, file.path(output_dir, "park_factors_triple_by_half.csv"), row.names = FALSE, na = "")
utils::write.csv(park_factors_triple_overall, file.path(output_dir, "park_factors_triple_overall.csv"), row.names = FALSE, na = "")
utils::write.csv(park_factors_distance_half, file.path(output_dir, "park_factors_distance_by_half.csv"), row.names = FALSE, na = "")
utils::write.csv(park_factors_distance_overall, file.path(output_dir, "park_factors_distance_overall.csv"), row.names = FALSE, na = "")
utils::write.csv(park_factors_hand, file.path(output_dir, "park_factors_by_hand.csv"), row.names = FALSE, na = "")
utils::write.csv(component_results$bacon$hand, file.path(output_dir, "park_factors_bacon_by_hand.csv"), row.names = FALSE, na = "")
utils::write.csv(component_results$hr$hand, file.path(output_dir, "park_factors_hr_by_hand.csv"), row.names = FALSE, na = "")
utils::write.csv(component_results$xbh$hand, file.path(output_dir, "park_factors_xbh_by_hand.csv"), row.names = FALSE, na = "")
utils::write.csv(component_results$distance$hand, file.path(output_dir, "park_factors_distance_by_hand.csv"), row.names = FALSE, na = "")
utils::write.csv(component_hand_all, file.path(output_dir, "park_factors_components_by_hand.csv"), row.names = FALSE, na = "")
utils::write.csv(component_half_all, file.path(output_dir, "park_factors_components_by_half.csv"), row.names = FALSE, na = "")
utils::write.csv(component_overall_all, file.path(output_dir, "park_factors_components_overall.csv"), row.names = FALSE, na = "")
utils::write.csv(validation$summary, file.path(output_dir, "validation_summary.csv"), row.names = FALSE, na = "")
utils::write.csv(validation$detail, file.path(output_dir, "validation_detail.csv"), row.names = FALSE, na = "")
utils::write.csv(team_era$team_year, file.path(output_dir, "team_park_era_audit.csv"), row.names = FALSE, na = "")
utils::write.csv(team_era$transitions, file.path(output_dir, "team_park_transitions.csv"), row.names = FALSE, na = "")
utils::write.csv(invariance, file.path(output_dir, "invariance_checks.csv"), row.names = FALSE, na = "")
utils::write.csv(fixed_effects_all, file.path(output_dir, "park_factor_fixed_effects.csv"), row.names = FALSE, na = "")

run_meta <- data.frame(
  key = c(
    "run_timestamp_utc",
    "bbe_input",
    "defense_input",
    "drag_input",
    "max_date",
    "drag_coverage_share",
    "park_events_csv",
    "schedule_cache_csv",
    "min_season",
    "exclude_seasons",
    "train_window",
    "min_train_seasons",
    "measurement_era_mode",
    "measurement_era_used",
    "rows_modeled",
    "seasons_modeled",
    "bacon_component_available",
    "hr_component_available",
    "xbh_component_available",
    "double_component_available",
    "triple_component_available",
    "distance_component_available",
    "bacon_component_outcome_used",
    "hr_component_outcome_used",
    "xbh_component_outcome_used",
    "double_component_outcome_used",
    "triple_component_outcome_used",
    "distance_component_outcome_used"
  ),
  value = c(
    format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    bbe_input,
    defense_input,
    drag_input,
    ifelse(is.na(max_date), "", format(max_date)),
    sprintf("%.4f", drag_coverage),
    park_events_csv,
    schedule_cache_csv,
    as.character(min_season),
    paste(exclude_seasons, collapse = ","),
    as.character(train_window),
    as.character(min_train_seasons),
    measurement_era_mode,
    as.character(include_measurement_era),
    as.character(nrow(model_data)),
    paste(sort(unique(model_data$season)), collapse = ","),
    as.character(isTRUE(component_results$bacon$available)),
    as.character(isTRUE(component_results$hr$available)),
    as.character(isTRUE(component_results$xbh$available)),
    as.character(isTRUE(component_results$double$available)),
    as.character(isTRUE(component_results$triple$available)),
    as.character(isTRUE(component_results$distance$available)),
    as.character(component_results$bacon$outcome_used),
    as.character(component_results$hr$outcome_used),
    as.character(component_results$xbh$outcome_used),
    as.character(component_results$double$outcome_used),
    as.character(component_results$triple$outcome_used),
    as.character(component_results$distance$outcome_used)
  ),
  stringsAsFactors = FALSE
)
utils::write.csv(run_meta, file.path(output_dir, "run_metadata.csv"), row.names = FALSE, na = "")

message("Building Savant-style display table...")
display_cmd <- c(
  file.path("scripts", "park_factors", "build_park_factor_display.R"),
  "--output-dir", output_dir
)
display_status <- tryCatch(
  {
    out <- system2("Rscript", display_cmd, stdout = TRUE, stderr = TRUE)
    if (length(out) > 0) {
      message(paste(out, collapse = "\n"))
    }
    0L
  },
  error = function(e) {
    warning(sprintf("Failed to build Savant-style display table: %s", conditionMessage(e)))
    1L
  }
)
if (display_status != 0L) {
  warning("Savant-style display table step did not complete cleanly.")
}

message("Park factor build complete.")
message("Output directory: ", output_dir)
