#!/usr/bin/env Rscript

args_raw <- commandArgs(trailingOnly = TRUE)

config_path <- file.path("config", "pipeline.yml")
args <- character(0)
i <- 1
while (i <= length(args_raw)) {
  arg <- args_raw[[i]]
  if (arg == "--config" && i < length(args_raw)) {
    config_path <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--config=")) {
    config_path <- sub("^--config=", "", arg)
    i <- i + 1
    next
  }
  args <- c(args, arg)
  i <- i + 1
}

source(file.path("R", "pipeline_config.R"))

if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("Package 'jsonlite' is required. Install with install.packages('jsonlite').")
}

source(file.path("R", "fangraphs_projections.R"))

cfg <- load_pipeline_config(config_path)

arg1 <- if (length(args) >= 1) args[[1]] else ""
arg2 <- if (length(args) >= 2) args[[2]] else ""
arg3 <- if (length(args) >= 3) args[[3]] else ""
arg4 <- if (length(args) >= 4) args[[4]] else ""
arg5 <- if (length(args) >= 5) args[[5]] else ""
arg6 <- if (length(args) >= 6) args[[6]] else ""
arg7 <- if (length(args) >= 7) args[[7]] else ""
arg8 <- if (length(args) >= 8) args[[8]] else ""

# Keep backward-compatible positional args behavior.
season_arg <- ""
systems_arg <- ""
weights_arg <- ""
pa_floor_arg <- ""
total_budget_arg <- ""
hitter_budget_share_arg <- ""
min_bid_arg <- ""
num_teams_arg <- ""

if (nzchar(arg1) && grepl("^[0-9]{4}$", arg1)) {
  season_arg <- arg1
  if (nzchar(arg2) && grepl("=", arg2, fixed = TRUE)) {
    systems_arg <- ""
    weights_arg <- arg2
    pa_floor_arg <- arg3
    total_budget_arg <- arg4
    hitter_budget_share_arg <- arg5
    min_bid_arg <- arg6
    num_teams_arg <- arg7
  } else {
    systems_arg <- arg2
    if (nzchar(arg3) && grepl("=", arg3, fixed = TRUE)) {
      weights_arg <- arg3
      pa_floor_arg <- arg4
      total_budget_arg <- arg5
      hitter_budget_share_arg <- arg6
      min_bid_arg <- arg7
      num_teams_arg <- arg8
    } else {
      weights_arg <- ""
      pa_floor_arg <- arg3
      total_budget_arg <- arg4
      hitter_budget_share_arg <- arg5
      min_bid_arg <- arg6
      num_teams_arg <- arg7
    }
  }
} else {
  # If no season is passed, preserve old behavior with arg2+ slots.
  if (nzchar(arg1) && grepl("=", arg1, fixed = TRUE)) {
    weights_arg <- arg1
    pa_floor_arg <- arg2
    total_budget_arg <- arg3
    hitter_budget_share_arg <- arg4
    min_bid_arg <- arg5
    num_teams_arg <- arg6
  } else {
    systems_arg <- arg1
    if (nzchar(arg2) && grepl("=", arg2, fixed = TRUE)) {
      weights_arg <- arg2
      pa_floor_arg <- arg3
      total_budget_arg <- arg4
      hitter_budget_share_arg <- arg5
      min_bid_arg <- arg6
      num_teams_arg <- arg7
    } else {
      weights_arg <- ""
      pa_floor_arg <- arg2
      total_budget_arg <- arg3
      hitter_budget_share_arg <- arg4
      min_bid_arg <- arg5
      num_teams_arg <- arg6
    }
  }
}

parse_systems_arg <- function(arg, available_systems, default_systems) {
  if (!nzchar(arg)) {
    return(default_systems)
  }

  systems <- tolower(trimws(strsplit(arg, ",", fixed = TRUE)[[1]]))
  systems <- systems[nzchar(systems)]
  systems <- unique(systems)

  if (length(systems) == 0) {
    stop("System list cannot be empty.")
  }

  unknown <- setdiff(systems, available_systems)
  if (length(unknown) > 0) {
    stop(sprintf("Unknown projection systems: %s", paste(unknown, collapse = ", ")))
  }

  systems
}

parse_weights_arg <- function(arg, default_weights, required_systems) {
  if (!nzchar(arg)) {
    return(validate_projection_weights(default_weights, required_systems = required_systems))
  }

  out <- default_weights
  entries <- strsplit(arg, ",", fixed = TRUE)[[1]]

  for (entry in entries) {
    parts <- strsplit(entry, "=", fixed = TRUE)[[1]]
    if (length(parts) != 2) {
      stop("Weights must use key=value pairs, comma-separated.")
    }

    key <- tolower(trimws(parts[1]))
    value <- suppressWarnings(as.numeric(trimws(parts[2])))

    if (!key %in% names(out)) {
      stop(sprintf("Unknown projection system in weights: %s", key))
    }
    if (is.na(value)) {
      stop(sprintf("Weight for system '%s' is not numeric.", key))
    }

    out[[key]] <- value
  }

  validate_projection_weights(out, required_systems = required_systems)
}

parse_pa_floor_arg <- function(arg, default_floor = 200) {
  if (!nzchar(arg)) {
    return(default_floor)
  }

  floor_value <- suppressWarnings(as.numeric(trimws(arg)))
  if (is.na(floor_value) || floor_value < 0) {
    stop("pa_floor must be a non-negative number.")
  }

  floor_value
}

parse_total_budget_arg <- function(arg, default_budget = DEFAULT_TOTAL_BUDGET) {
  if (!nzchar(arg)) {
    return(default_budget)
  }

  budget_value <- suppressWarnings(as.numeric(trimws(arg)))
  if (is.na(budget_value) || budget_value <= 0) {
    stop("total_budget must be a positive number.")
  }

  budget_value
}

parse_hitter_budget_share_arg <- function(arg, default_share = DEFAULT_HITTER_BUDGET_SHARE) {
  if (!nzchar(arg)) {
    return(default_share)
  }

  share_value <- suppressWarnings(as.numeric(trimws(arg)))
  if (is.na(share_value) || share_value <= 0 || share_value >= 1) {
    stop("hitter_budget_share must be between 0 and 1.")
  }

  share_value
}

parse_min_bid_arg <- function(arg, default_min_bid = DEFAULT_MIN_BID) {
  if (!nzchar(arg)) {
    return(default_min_bid)
  }

  min_bid_value <- suppressWarnings(as.numeric(trimws(arg)))
  if (is.na(min_bid_value) || min_bid_value < 0) {
    stop("min_bid must be a non-negative number.")
  }

  min_bid_value
}

parse_num_teams_arg <- function(arg, default_num_teams = DEFAULT_NFBC_TEAMS) {
  if (!nzchar(arg)) {
    return(default_num_teams)
  }

  value <- suppressWarnings(as.numeric(trimws(arg)))
  if (is.na(value) || value < 1) {
    stop("num_teams must be a positive number.")
  }

  value
}

season <- if (nzchar(season_arg)) as.integer(season_arg) else as.integer(cfg$season)
selected_systems <- parse_systems_arg(systems_arg, names(FG_SYSTEM_CODES), cfg$projection$systems)
weights <- parse_weights_arg(weights_arg, cfg$projection$system_weights, required_systems = selected_systems)
pa_floor <- parse_pa_floor_arg(pa_floor_arg, default_floor = cfg$projection$pa_floor)
total_budget <- parse_total_budget_arg(total_budget_arg, default_budget = cfg$projection$auction$total_budget)
hitter_budget_share <- parse_hitter_budget_share_arg(hitter_budget_share_arg, default_share = cfg$projection$auction$hitter_budget_share)
min_bid <- parse_min_bid_arg(min_bid_arg, default_min_bid = cfg$projection$auction$min_bid)
num_teams <- parse_num_teams_arg(num_teams_arg, default_num_teams = cfg$projection$league$num_teams)

raw_dir <- cfg$paths$raw_dir
processed_dir <- cfg$paths$processed_dir

ensure_dir <- function(path) {
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE, showWarnings = FALSE)
  }
}

ensure_dir(raw_dir)
ensure_dir(processed_dir)

adp_local_tsv_path <- cfg$adp$local_tsv
if (!nzchar(adp_local_tsv_path) || !file.exists(adp_local_tsv_path)) {
  adp_local_tsv_path <- ""
}

result <- fetch_all_systems(
  season = season,
  systems = selected_systems,
  weights = weights,
  pa_floor = pa_floor,
  category_weights = cfg$projection$category_weights,
  starter_count = cfg$projection$starter_count,
  starter_rank_metric = cfg$projection$starter_rank_metric,
  num_teams = num_teams,
  total_budget = total_budget,
  hitter_budget_share = hitter_budget_share,
  min_bid = min_bid,
  adp_local_tsv_path = adp_local_tsv_path,
  adp_fill_missing_with_max = isTRUE(cfg$adp$fill_missing_with_max),
  adp_lookback_days = cfg$adp$lookback_days,
  adp_draft_type = cfg$adp$draft_type,
  adp_num_teams = cfg$adp$num_teams,
  adp_raw_output_path = file.path(raw_dir, sprintf("%s_nfbc_adp.tsv", season)),
  player_match_overrides_path = cfg$paths$match_overrides_csv,
  quality = cfg$quality
)

timestamp <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

for (system_name in names(result$raw)) {
  raw_path <- file.path(raw_dir, sprintf("%s_%s_hitters_raw.csv", season, system_name))
  standard_path <- file.path(processed_dir, sprintf("%s_%s_hitters_standard.csv", season, system_name))
  per_pa_path <- file.path(processed_dir, sprintf("%s_%s_hitters_per_pa.csv", season, system_name))

  utils::write.csv(result$raw[[system_name]], raw_path, row.names = FALSE, na = "")
  utils::write.csv(result$standard[[system_name]], standard_path, row.names = FALSE, na = "")
  utils::write.csv(result$per_pa[[system_name]], per_pa_path, row.names = FALSE, na = "")
}

combined_standard_path <- file.path(processed_dir, sprintf("%s_hitters_standard_all_systems.csv", season))
combined_per_pa_path <- file.path(processed_dir, sprintf("%s_hitters_per_pa_all_systems.csv", season))
weighted_aggregate_path <- file.path(processed_dir, sprintf("%s_hitters_weighted_per_pa.csv", season))
zscore_path <- file.path(processed_dir, sprintf("%s_hitters_weighted_per_pa_zscores.csv", season))
zscore_reference_path <- file.path(processed_dir, sprintf("%s_hitters_weighted_per_pa_zscore_reference.csv", season))
starter_pool_path <- file.path(processed_dir, sprintf("%s_hitters_weighted_per_pa_starter_pool.csv", season))
dollar_reference_path <- file.path(processed_dir, sprintf("%s_hitters_weighted_per_pa_dollar_reference.csv", season))
z_scored_output_path <- file.path(processed_dir, sprintf("%s_hitters_z_scored_aggregate_projection_output.csv", season))
nfbc_adp_clean_path <- file.path(processed_dir, sprintf("%s_nfbc_adp_clean.csv", season))
adp_match_summary_path <- file.path(processed_dir, sprintf("%s_adp_match_summary.csv", season))
adp_projection_unmatched_path <- file.path(processed_dir, sprintf("%s_adp_projection_unmatched.csv", season))
adp_unmatched_path <- file.path(processed_dir, sprintf("%s_adp_unmatched.csv", season))
adp_matches_path <- file.path(processed_dir, sprintf("%s_adp_matches.csv", season))
metadata_path <- file.path(processed_dir, sprintf("%s_refresh_metadata.csv", season))

utils::write.csv(result$combined_standard, combined_standard_path, row.names = FALSE, na = "")
utils::write.csv(result$combined_per_pa, combined_per_pa_path, row.names = FALSE, na = "")
utils::write.csv(result$weighted_aggregate, weighted_aggregate_path, row.names = FALSE, na = "")
utils::write.csv(result$z_scores, zscore_path, row.names = FALSE, na = "")
utils::write.csv(result$zscore_reference, zscore_reference_path, row.names = FALSE, na = "")
utils::write.csv(result$starter_pool, starter_pool_path, row.names = FALSE, na = "")
utils::write.csv(result$dollar_reference, dollar_reference_path, row.names = FALSE, na = "")
utils::write.csv(result$nfbc_adp, nfbc_adp_clean_path, row.names = FALSE, na = "")
utils::write.csv(result$z_scored_aggregate_projection_output, z_scored_output_path, row.names = FALSE, na = "")

if (!is.null(result$adp_match_audit) && is.list(result$adp_match_audit)) {
  utils::write.csv(result$adp_match_audit$summary, adp_match_summary_path, row.names = FALSE, na = "")
  utils::write.csv(result$adp_match_audit$projection_unmatched, adp_projection_unmatched_path, row.names = FALSE, na = "")
  utils::write.csv(result$adp_match_audit$adp_unmatched, adp_unmatched_path, row.names = FALSE, na = "")
  utils::write.csv(result$adp_match_audit$matches, adp_matches_path, row.names = FALSE, na = "")
}

adp_match_rate <- NA_real_
adp_matched_rows <- NA_real_
if (!is.null(result$adp_match_audit$summary) && nrow(result$adp_match_audit$summary) > 0) {
  adp_match_rate <- suppressWarnings(as.numeric(result$adp_match_audit$summary$match_rate[1]))
  adp_matched_rows <- suppressWarnings(as.numeric(result$adp_match_audit$summary$matched_rows[1]))
}

metadata <- data.frame(
  season = season,
  refreshed_at_utc = timestamp,
  config_path = config_path,
  systems = paste(selected_systems, collapse = ","),
  weights = paste(sprintf("%s=%s", selected_systems, as.numeric(weights[selected_systems])), collapse = ","),
  category_weights = paste(sprintf("%s=%s", names(result$category_weights), as.numeric(result$category_weights)), collapse = ","),
  pa_floor = pa_floor,
  starter_count = result$starter_count,
  starter_rank_metric = result$starter_rank_metric,
  num_teams = result$num_teams,
  total_budget = result$total_budget,
  hitter_budget_share = result$hitter_budget_share,
  budget_bats = result$total_budget * result$hitter_budget_share * result$num_teams,
  min_bid = result$min_bid,
  records_combined_standard = nrow(result$combined_standard),
  records_combined_per_pa = nrow(result$combined_per_pa),
  records_weighted_aggregate = nrow(result$weighted_aggregate),
  records_z_scores = nrow(result$z_scores),
  records_starter_pool = nrow(result$starter_pool),
  records_nfbc_adp = nrow(result$nfbc_adp),
  nfbc_adp_source = result$nfbc_adp_source,
  nfbc_adp_source_path = result$nfbc_adp_source_path,
  nfbc_adp_from_date = result$nfbc_adp_from_date,
  nfbc_adp_to_date = result$nfbc_adp_to_date,
  nfbc_adp_draft_type = result$nfbc_adp_draft_type,
  nfbc_adp_num_teams = result$nfbc_adp_num_teams,
  adp_match_rate = adp_match_rate,
  adp_matched_rows = adp_matched_rows,
  records_z_scored_aggregate_projection_output = nrow(result$z_scored_aggregate_projection_output),
  stringsAsFactors = FALSE
)

utils::write.csv(metadata, metadata_path, row.names = FALSE, na = "")

message("Projection refresh complete.")
message(sprintf("Config: %s", config_path))
message(sprintf("Combined standard rows: %s", nrow(result$combined_standard)))
message(sprintf("Combined per-PA rows: %s", nrow(result$combined_per_pa)))
message(sprintf("Weighted aggregate rows: %s", nrow(result$weighted_aggregate)))
message(sprintf("Z-score rows (PA >= %s): %s", pa_floor, nrow(result$z_scores)))
message(sprintf("Starter pool rows: %s", nrow(result$starter_pool)))
message(sprintf("NFBC ADP rows: %s (%s)", nrow(result$nfbc_adp), result$nfbc_adp_source))
message(sprintf("ADP match rate: %s", ifelse(is.na(adp_match_rate), "NA", sprintf("%.3f", adp_match_rate))))
message(sprintf("Weights used: %s", metadata$weights[1]))
message(sprintf("Category weights used: %s", metadata$category_weights[1]))
message(sprintf(
  "Budget settings: total_budget=%s, num_teams=%s, hitter_budget_share=%s, budget_bats=%s, min_bid=%s",
  result$total_budget,
  result$num_teams,
  result$hitter_budget_share,
  result$total_budget * result$hitter_budget_share * result$num_teams,
  result$min_bid
))
