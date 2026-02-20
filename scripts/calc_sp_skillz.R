#!/usr/bin/env Rscript

args_raw <- commandArgs(trailingOnly = TRUE)

config_path <- file.path("config", "pipeline.yml")
season_arg <- ""
source_url_arg <- ""
input_csv_arg <- ""
num_teams_arg <- ""
sp_depth_arg <- ""
start_share_min_arg <- ""
use_ip_paradigms_arg <- ""
low_ip_max_start_ip_arg <- ""
high_ip_min_start_ip_arg <- ""
gs_fallback_threshold_arg <- ""
weights_arg <- ""
low_ip_weights_arg <- ""
high_ip_weights_arg <- ""
stabilization_points_arg <- ""
reliability_method_arg <- ""
eno_sheet_url_arg <- ""
eno_sheet_tab_arg <- ""
include_eno_stuff_arg <- ""

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
  if (arg == "--season" && i < length(args_raw)) {
    season_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--season=")) {
    season_arg <- sub("^--season=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--source-url" && i < length(args_raw)) {
    source_url_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--source-url=")) {
    source_url_arg <- sub("^--source-url=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--input-csv" && i < length(args_raw)) {
    input_csv_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--input-csv=")) {
    input_csv_arg <- sub("^--input-csv=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--num-teams" && i < length(args_raw)) {
    num_teams_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--num-teams=")) {
    num_teams_arg <- sub("^--num-teams=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--sp-depth" && i < length(args_raw)) {
    sp_depth_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--sp-depth=")) {
    sp_depth_arg <- sub("^--sp-depth=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--start-share-min" && i < length(args_raw)) {
    start_share_min_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--start-share-min=")) {
    start_share_min_arg <- sub("^--start-share-min=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--use-ip-paradigms" && i < length(args_raw)) {
    use_ip_paradigms_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--use-ip-paradigms=")) {
    use_ip_paradigms_arg <- sub("^--use-ip-paradigms=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--low-ip-max-start-ip" && i < length(args_raw)) {
    low_ip_max_start_ip_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--low-ip-max-start-ip=")) {
    low_ip_max_start_ip_arg <- sub("^--low-ip-max-start-ip=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--high-ip-min-start-ip" && i < length(args_raw)) {
    high_ip_min_start_ip_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--high-ip-min-start-ip=")) {
    high_ip_min_start_ip_arg <- sub("^--high-ip-min-start-ip=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--gs-fallback-threshold" && i < length(args_raw)) {
    gs_fallback_threshold_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--gs-fallback-threshold=")) {
    gs_fallback_threshold_arg <- sub("^--gs-fallback-threshold=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--weights" && i < length(args_raw)) {
    weights_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--weights=")) {
    weights_arg <- sub("^--weights=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--low-ip-weights" && i < length(args_raw)) {
    low_ip_weights_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--low-ip-weights=")) {
    low_ip_weights_arg <- sub("^--low-ip-weights=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--high-ip-weights" && i < length(args_raw)) {
    high_ip_weights_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--high-ip-weights=")) {
    high_ip_weights_arg <- sub("^--high-ip-weights=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--stabilization-points" && i < length(args_raw)) {
    stabilization_points_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--stabilization-points=")) {
    stabilization_points_arg <- sub("^--stabilization-points=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--reliability-method" && i < length(args_raw)) {
    reliability_method_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--reliability-method=")) {
    reliability_method_arg <- sub("^--reliability-method=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--eno-sheet-url" && i < length(args_raw)) {
    eno_sheet_url_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--eno-sheet-url=")) {
    eno_sheet_url_arg <- sub("^--eno-sheet-url=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--eno-sheet-tab" && i < length(args_raw)) {
    eno_sheet_tab_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--eno-sheet-tab=")) {
    eno_sheet_tab_arg <- sub("^--eno-sheet-tab=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--include-eno-stuff" && i < length(args_raw)) {
    include_eno_stuff_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--include-eno-stuff=")) {
    include_eno_stuff_arg <- sub("^--include-eno-stuff=", "", arg)
    i <- i + 1
    next
  }
  stop(sprintf("Unknown argument: %s", arg))
}

source(file.path("R", "pipeline_config.R"))
source(file.path("R", "sp_skillz.R"))

cfg <- load_pipeline_config(config_path)
sp_cfg <- cfg$pitcher$sp_skillz

parse_num <- function(x, fallback) {
  if (!nzchar(x)) {
    return(fallback)
  }
  val <- suppressWarnings(as.numeric(trimws(x)))
  if (length(val) != 1 || is.na(val)) {
    stop(sprintf("Invalid numeric value: %s", x))
  }
  val
}

parse_int <- function(x, fallback) {
  as.integer(round(parse_num(x, fallback)))
}

parse_bool <- function(x, fallback) {
  if (!nzchar(x)) {
    return(isTRUE(fallback))
  }
  key <- tolower(trimws(x))
  if (key %in% c("1", "true", "t", "yes", "y")) {
    return(TRUE)
  }
  if (key %in% c("0", "false", "f", "no", "n")) {
    return(FALSE)
  }
  stop(sprintf("Invalid boolean value: %s", x))
}

weights_to_text <- function(weights_vec) {
  paste(sprintf("%s=%s", names(weights_vec), as.numeric(weights_vec)), collapse = ",")
}

parse_weights <- function(weights_text, default_weights) {
  weights <- validate_sp_skillz_weights(default_weights)
  if (!nzchar(weights_text)) {
    return(weights)
  }

  entries <- strsplit(weights_text, ",", fixed = TRUE)[[1]]
  for (entry in entries) {
    parts <- strsplit(entry, "=", fixed = TRUE)[[1]]
    if (length(parts) != 2) {
      stop("Weights must use comma-separated key=value pairs.")
    }
    key <- trimws(parts[1])
    value <- suppressWarnings(as.numeric(trimws(parts[2])))
    if (!key %in% names(weights)) {
      stop(sprintf("Unknown SP Skillz weight metric: %s", key))
    }
    if (is.na(value)) {
      stop(sprintf("Weight for metric '%s' is not numeric.", key))
    }
    weights[[key]] <- value
  }

  validate_sp_skillz_weights(weights)
}

parse_stabilization_points <- function(points_text, default_points) {
  points <- validate_sp_skillz_stabilization_points(default_points)
  if (!nzchar(points_text)) {
    return(points)
  }

  entries <- strsplit(points_text, ",", fixed = TRUE)[[1]]
  for (entry in entries) {
    parts <- strsplit(entry, "=", fixed = TRUE)[[1]]
    if (length(parts) != 2) {
      stop("stabilization_points must use comma-separated key=value pairs.")
    }
    key <- trimws(parts[1])
    value <- suppressWarnings(as.numeric(trimws(parts[2])))
    if (!key %in% names(points)) {
      stop(sprintf("Unknown stabilization metric: %s", key))
    }
    if (is.na(value) || value < 0) {
      stop(sprintf("Stabilization point for metric '%s' must be a non-negative number.", key))
    }
    points[[key]] <- value
  }

  validate_sp_skillz_stabilization_points(points)
}

parse_reliability_method <- function(x, fallback = "sample_over_sample_plus_stab") {
  allowed <- c("sample_over_sample_plus_stab", "linear_cap")
  val <- if (nzchar(x)) trimws(x) else as.character(fallback)
  if (!val %in% allowed) {
    stop(sprintf("Invalid reliability method '%s'. Allowed: %s", val, paste(allowed, collapse = ", ")))
  }
  val
}

lookup_col <- function(df, candidates) {
  out <- select_first_match(df, candidates)
  if (all(is.na(out))) {
    rep(NA, nrow(df))
  } else {
    out
  }
}

read_eno_rankings <- function(sheet_url, sheet_tab) {
  if (!nzchar(sheet_url)) {
    return(NULL)
  }
  if (!requireNamespace("googlesheets4", quietly = TRUE)) {
    stop("Package 'googlesheets4' is required for Eno sheet integration.")
  }

  googlesheets4::gs4_deauth()
  eno_raw <- googlesheets4::read_sheet(sheet_url, sheet = sheet_tab)
  if (nrow(eno_raw) == 0) {
    return(NULL)
  }

  eno <- data.frame(
    player_id = as.integer(round(as_numeric_clean(lookup_col(eno_raw, c("mlbam id", "mlbamid", "playerid", "pitcher", "xmlbamid"))))),
    player_name = strip_html_tags(lookup_col(eno_raw, c("name", "player_name", "player", "pitcher"))),
    eno_rank = as_numeric_clean(lookup_col(eno_raw, c("eno", "rank"))),
    eno_2026_stuff_plus = as_numeric_clean(lookup_col(eno_raw, c("stuff+", "s+", "sp_stuff"))),
    eno_2026_pitching_plus = as_numeric_clean(lookup_col(eno_raw, c("pitching+", "p+", "sp_pitching"))),
    eno_2026_location_plus = as_numeric_clean(lookup_col(eno_raw, c("location+", "l+", "sp_location"))),
    stringsAsFactors = FALSE
  )

  eno$player_name[is.na(eno$player_name)] <- ""
  eno$player_name <- trimws(eno$player_name)
  eno <- eno[nzchar(eno$player_name) | !is.na(eno$player_id), , drop = FALSE]
  if (nrow(eno) == 0) {
    return(NULL)
  }

  eno$name_key <- normalize_col_key(eno$player_name)
  eno <- eno[order(is.na(eno$eno_rank), eno$eno_rank, eno$player_name), , drop = FALSE]
  rownames(eno) <- NULL
  eno
}

merge_eno_rankings <- function(scores, eno_rankings) {
  out <- scores
  out$eno_rank <- NA_real_
  out$eno_2026_stuff_plus <- NA_real_
  out$eno_2026_pitching_plus <- NA_real_
  out$eno_2026_location_plus <- NA_real_
  out$eno_match_type <- "unmatched"
  out$ohtani_rule <- FALSE

  if (is.null(eno_rankings) || nrow(eno_rankings) == 0) {
    return(out)
  }

  by_id <- eno_rankings[!is.na(eno_rankings$player_id), , drop = FALSE]
  by_id <- by_id[!duplicated(by_id$player_id), , drop = FALSE]
  by_name <- eno_rankings[nzchar(eno_rankings$name_key), , drop = FALSE]
  by_name <- by_name[!duplicated(by_name$name_key), , drop = FALSE]

  if ("player_id" %in% names(out)) {
    idx_id <- match(out$player_id, by_id$player_id)
    matched_id <- !is.na(out$player_id) & !is.na(idx_id)
    if (any(matched_id)) {
      out$eno_rank[matched_id] <- by_id$eno_rank[idx_id[matched_id]]
      out$eno_2026_stuff_plus[matched_id] <- by_id$eno_2026_stuff_plus[idx_id[matched_id]]
      out$eno_2026_pitching_plus[matched_id] <- by_id$eno_2026_pitching_plus[idx_id[matched_id]]
      out$eno_2026_location_plus[matched_id] <- by_id$eno_2026_location_plus[idx_id[matched_id]]
      out$eno_match_type[matched_id] <- "id"
    }
  }

  name_key <- normalize_col_key(out$player_name)
  remaining <- out$eno_match_type == "unmatched" & nzchar(name_key)
  if (any(remaining)) {
    idx_name <- match(name_key[remaining], by_name$name_key)
    matched_name <- !is.na(idx_name)
    if (any(matched_name)) {
      row_idx <- which(remaining)[matched_name]
      ref_idx <- idx_name[matched_name]
      out$eno_rank[row_idx] <- by_name$eno_rank[ref_idx]
      out$eno_2026_stuff_plus[row_idx] <- by_name$eno_2026_stuff_plus[ref_idx]
      out$eno_2026_pitching_plus[row_idx] <- by_name$eno_2026_pitching_plus[ref_idx]
      out$eno_2026_location_plus[row_idx] <- by_name$eno_2026_location_plus[ref_idx]
      out$eno_match_type[row_idx] <- "name"
    }
  }

  # Ohtani Rule: always keep him in pitcher outputs if present in Eno rankings.
  ohtani_key <- normalize_col_key("Shohei Ohtani")
  ohtani_in_output <- any(normalize_col_key(out$player_name) == ohtani_key)
  ohtani_ref <- by_name[by_name$name_key == ohtani_key, , drop = FALSE]
  if (!ohtani_in_output && nrow(ohtani_ref) > 0) {
    missing_row <- out[1, , drop = FALSE]
    missing_row[1, ] <- NA
    missing_row$player_id <- ohtani_ref$player_id[1]
    missing_row$player_name <- "Shohei Ohtani"
    missing_row$team <- ""
    missing_row$lookback_season <- if ("lookback_season" %in% names(out)) out$lookback_season[1] else NA
    missing_row$eno_rank <- ohtani_ref$eno_rank[1]
    missing_row$eno_2026_stuff_plus <- ohtani_ref$eno_2026_stuff_plus[1]
    missing_row$eno_2026_pitching_plus <- ohtani_ref$eno_2026_pitching_plus[1]
    missing_row$eno_2026_location_plus <- ohtani_ref$eno_2026_location_plus[1]
    missing_row$eno_match_type <- "ohtani_rule"
    missing_row$ohtani_rule <- TRUE
    out <- rbind(out, missing_row)
  } else if (ohtani_in_output) {
    out$ohtani_rule[normalize_col_key(out$player_name) == ohtani_key] <- TRUE
  }

  out
}

lookback_season <- parse_int(season_arg, sp_cfg$lookback_season %||% (cfg$season - 1))
source_url <- if (nzchar(source_url_arg)) source_url_arg else (sp_cfg$source_url %||% "")
local_csv_path <- input_csv_arg
num_teams <- parse_int(num_teams_arg, sp_cfg$num_teams %||% cfg$projection$league$num_teams)
sp_depth <- parse_num(sp_depth_arg, sp_cfg$sp_depth %||% 10)
start_share_min <- parse_num(start_share_min_arg, sp_cfg$start_share_min %||% (2 / 3))
use_ip_paradigms <- parse_bool(use_ip_paradigms_arg, sp_cfg$use_ip_paradigms %||% TRUE)
low_ip_max_start_ip <- parse_num(low_ip_max_start_ip_arg, sp_cfg$low_ip_max_start_ip %||% 80)
high_ip_min_start_ip <- parse_num(high_ip_min_start_ip_arg, sp_cfg$high_ip_min_start_ip %||% 100)
gs_fallback_threshold <- parse_num(gs_fallback_threshold_arg, sp_cfg$gs_fallback_threshold %||% 5)

weights <- parse_weights(
  weights_text = weights_arg,
  default_weights = sp_cfg$weights %||% DEFAULT_SP_SKILLZ_WEIGHTS
)
low_ip_weights <- parse_weights(
  weights_text = low_ip_weights_arg,
  default_weights = sp_cfg$low_ip_weights %||% DEFAULT_LOW_IP_PARADIGM_WEIGHTS
)
high_ip_weights <- parse_weights(
  weights_text = high_ip_weights_arg,
  default_weights = sp_cfg$high_ip_weights %||% DEFAULT_HIGH_IP_PARADIGM_WEIGHTS
)
stabilization_points <- parse_stabilization_points(
  points_text = stabilization_points_arg,
  default_points = sp_cfg$stabilization_points %||% DEFAULT_SP_SKILLZ_STABILIZATION_POINTS
)
reliability_method <- parse_reliability_method(
  reliability_method_arg,
  sp_cfg$reliability_method %||% "sample_over_sample_plus_stab"
)

eno_sheet_url <- if (nzchar(eno_sheet_url_arg)) eno_sheet_url_arg else (sp_cfg$eno$sheet_url %||% "")
eno_sheet_tab <- if (nzchar(eno_sheet_tab_arg)) eno_sheet_tab_arg else (sp_cfg$eno$sheet_tab %||% "Feb 3 Ranks")
include_eno_stuff <- parse_bool(include_eno_stuff_arg, sp_cfg$eno$include_2026_stuff_plus %||% TRUE)

if (!nzchar(local_csv_path) && !nzchar(source_url)) {
  stop("Either --input-csv must be provided or pitcher.sp_skillz.source_url must be configured.")
}

dir.create(cfg$paths$raw_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(cfg$paths$processed_dir, recursive = TRUE, showWarnings = FALSE)

result <- fetch_and_score_sp_skillz(
  leaderboard_url = source_url,
  local_csv_path = local_csv_path,
  lookback_season = lookback_season,
  weights = weights,
  num_teams = num_teams,
  sp_depth = sp_depth,
  start_share_min = start_share_min,
  use_ip_paradigms = use_ip_paradigms,
  low_ip_max_start_ip = low_ip_max_start_ip,
  high_ip_min_start_ip = high_ip_min_start_ip,
  low_ip_weights = low_ip_weights,
  high_ip_weights = high_ip_weights,
  gs_fallback_threshold = gs_fallback_threshold,
  stabilization_points = stabilization_points,
  reliability_method = reliability_method
)

eno_rankings <- NULL
if (isTRUE(include_eno_stuff) && nzchar(eno_sheet_url)) {
  eno_rankings <- read_eno_rankings(eno_sheet_url, eno_sheet_tab)
}
scores_with_eno <- merge_eno_rankings(result$scores, eno_rankings)
scores_with_eno <- scores_with_eno[order(scores_with_eno$sp_skillz_rank, scores_with_eno$player_name, na.last = TRUE), , drop = FALSE]
rownames(scores_with_eno) <- NULL

raw_out <- file.path(cfg$paths$raw_dir, sprintf("%s_sp_skillz_raw.csv", lookback_season))
standard_out <- file.path(cfg$paths$processed_dir, sprintf("%s_sp_skillz_standardized.csv", lookback_season))
scores_out <- file.path(cfg$paths$processed_dir, sprintf("%s_sp_skillz_scores.csv", lookback_season))
starter_pool_out <- file.path(cfg$paths$processed_dir, sprintf("%s_sp_skillz_starter_pool.csv", lookback_season))
reference_out <- file.path(cfg$paths$processed_dir, sprintf("%s_sp_skillz_reference.csv", lookback_season))
metadata_out <- file.path(cfg$paths$processed_dir, sprintf("%s_sp_skillz_metadata.csv", lookback_season))
eno_out <- file.path(cfg$paths$processed_dir, sprintf("%s_sp_skillz_eno_rankings.csv", lookback_season))

utils::write.csv(result$raw, raw_out, row.names = FALSE, na = "")
utils::write.csv(result$standardized, standard_out, row.names = FALSE, na = "")
utils::write.csv(scores_with_eno, scores_out, row.names = FALSE, na = "")
utils::write.csv(result$starter_pool, starter_pool_out, row.names = FALSE, na = "")
utils::write.csv(result$reference, reference_out, row.names = FALSE, na = "")
if (!is.null(eno_rankings)) {
  utils::write.csv(eno_rankings, eno_out, row.names = FALSE, na = "")
}

metadata <- data.frame(
  run_at_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  lookback_season = as.integer(lookback_season),
  source_url = source_url,
  local_csv_path = local_csv_path,
  num_teams = as.integer(num_teams),
  sp_depth = as.numeric(sp_depth),
  start_share_min = as.numeric(start_share_min),
  use_ip_paradigms = isTRUE(use_ip_paradigms),
  low_ip_max_start_ip = as.numeric(low_ip_max_start_ip),
  high_ip_min_start_ip = as.numeric(high_ip_min_start_ip),
  gs_fallback_threshold = as.numeric(gs_fallback_threshold),
  weights_default = weights_to_text(weights),
  weights_low_ip = weights_to_text(low_ip_weights),
  weights_high_ip = weights_to_text(high_ip_weights),
  stabilization_points = weights_to_text(stabilization_points),
  reliability_method = reliability_method,
  reliability_median = suppressWarnings(stats::median(scores_with_eno$sp_skillz_reliability, na.rm = TRUE)),
  reliability_p10 = suppressWarnings(stats::quantile(scores_with_eno$sp_skillz_reliability, probs = 0.10, na.rm = TRUE, names = FALSE)),
  reliability_p90 = suppressWarnings(stats::quantile(scores_with_eno$sp_skillz_reliability, probs = 0.90, na.rm = TRUE, names = FALSE)),
  eno_sheet_url = eno_sheet_url,
  eno_sheet_tab = eno_sheet_tab,
  include_eno_stuff = isTRUE(include_eno_stuff),
  eno_matches_id = sum(scores_with_eno$eno_match_type == "id", na.rm = TRUE),
  eno_matches_name = sum(scores_with_eno$eno_match_type == "name", na.rm = TRUE),
  eno_unmatched = sum(scores_with_eno$eno_match_type == "unmatched", na.rm = TRUE),
  ohtani_rule_rows = sum(isTRUE(scores_with_eno$ohtani_rule), na.rm = TRUE),
  rows_raw = nrow(result$raw),
  rows_after_filter = nrow(result$scores),
  rows_after_eno_merge = nrow(scores_with_eno),
  rows_starter_pool = nrow(result$starter_pool),
  stringsAsFactors = FALSE
)
utils::write.csv(metadata, metadata_out, row.names = FALSE, na = "")

message(sprintf("Saved raw SP Skillz input: %s", raw_out))
message(sprintf("Saved standardized SP Skillz input: %s", standard_out))
message(sprintf("Saved SP Skillz scores: %s", scores_out))
message(sprintf("Saved SP Skillz starter pool: %s", starter_pool_out))
message(sprintf("Saved SP Skillz reference: %s", reference_out))
message(sprintf("Saved SP Skillz metadata: %s", metadata_out))
if (!is.null(eno_rankings)) {
  message(sprintf("Saved Eno rankings sample: %s", eno_out))
}
message(sprintf("SP Skillz complete: %s scored pitchers (%s after Eno merge).", nrow(result$scores), nrow(scores_with_eno)))
