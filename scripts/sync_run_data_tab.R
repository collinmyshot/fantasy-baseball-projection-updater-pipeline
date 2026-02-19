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
source(file.path("R", "gsheets_auth.R"))

cfg <- load_pipeline_config(config_path)

sheet_url <- if (length(args) >= 1) args[[1]] else cfg$google_sheets$workbook_url
run_data_tab <- if (length(args) >= 2) args[[2]] else cfg$google_sheets$run_data_tab
projection_tab <- if (length(args) >= 3) args[[3]] else cfg$google_sheets$projection_tab

if (!nzchar(sheet_url)) {
  stop("Usage: Rscript scripts/sync_run_data_tab.R <google_sheet_url> [run_data_tab] [projection_tab] [--config path]")
}

if (!requireNamespace("googlesheets4", quietly = TRUE)) {
  stop("Package 'googlesheets4' is required.")
}

safe_read_csv <- function(path) {
  if (!nzchar(path) || !file.exists(path)) {
    return(NULL)
  }
  tryCatch(utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE), error = function(e) NULL)
}

parse_weight_pairs <- function(pair_text) {
  if (!nzchar(pair_text) || is.na(pair_text)) {
    return(data.frame(metric = character(0), value = character(0), stringsAsFactors = FALSE))
  }

  parts <- trimws(unlist(strsplit(pair_text, ",", fixed = TRUE)))
  parts <- parts[nzchar(parts)]
  if (length(parts) == 0) {
    return(data.frame(metric = character(0), value = character(0), stringsAsFactors = FALSE))
  }

  items <- lapply(parts, function(p) {
    kv <- trimws(unlist(strsplit(p, "=", fixed = TRUE)))
    if (length(kv) < 2) {
      return(c(metric = p, value = ""))
    }
    c(metric = kv[1], value = paste(kv[-1], collapse = "="))
  })

  out <- do.call(rbind, items)
  data.frame(metric = as.character(out[, "metric"]), value = as.character(out[, "value"]), stringsAsFactors = FALSE)
}

adp_draft_type_label <- function(draft_type_id) {
  id <- suppressWarnings(as.integer(draft_type_id))
  labels <- c(
    `897` = "NFBC 50",
    `896` = "Draft Champions",
    `895` = "Main Event",
    `899` = "Online Championship"
  )
  if (!is.na(id) && as.character(id) %in% names(labels)) {
    return(labels[[as.character(id)]])
  }
  if (!is.na(id)) {
    return(sprintf("Draft Type %s", id))
  }
  "Unknown"
}

build_rows <- function(refresh_meta, adp_meta, cfg) {
  season <- as.integer(cfg$season)
  refreshed_at <- NA_character_
  systems <- tolower(paste(cfg$projection$systems, collapse = ","))
  system_weights_text <- paste(sprintf("%s=%s", names(cfg$projection$system_weights), as.numeric(cfg$projection$system_weights)), collapse = ",")
  category_weights_text <- paste(sprintf("%s=%s", names(cfg$projection$category_weights), as.numeric(cfg$projection$category_weights)), collapse = ",")
  adp_source <- "config"

  if (!is.null(refresh_meta) && nrow(refresh_meta) > 0) {
    r <- refresh_meta[1, , drop = FALSE]
    season <- suppressWarnings(as.integer(r$season[1]))
    refreshed_at <- as.character(r$refreshed_at_utc[1])
    if (nzchar(as.character(r$systems[1]))) systems <- as.character(r$systems[1])
    if (nzchar(as.character(r$weights[1]))) system_weights_text <- as.character(r$weights[1])
    if (nzchar(as.character(r$category_weights[1]))) category_weights_text <- as.character(r$category_weights[1])
    if (nzchar(as.character(r$nfbc_adp_source[1]))) adp_source <- as.character(r$nfbc_adp_source[1])
  }

  adp_from <- NA_character_
  adp_to <- NA_character_
  adp_draft_type <- cfg$adp$draft_type
  adp_num_teams <- cfg$adp$num_teams
  adp_downloaded_at <- NA_character_
  adp_download_status <- "unknown"

  if (!is.null(adp_meta) && nrow(adp_meta) > 0) {
    a <- adp_meta[1, , drop = FALSE]
    adp_from <- as.character(a$from_date[1])
    adp_to <- as.character(a$to_date[1])
    adp_draft_type <- suppressWarnings(as.integer(a$draft_type[1]))
    adp_num_teams <- suppressWarnings(as.integer(a$num_teams[1]))
    adp_downloaded_at <- as.character(a$downloaded_at_utc[1])
    adp_download_status <- as.character(a$status[1])
  }

  sys_pairs <- parse_weight_pairs(system_weights_text)
  cat_pairs <- parse_weight_pairs(category_weights_text)

  rows <- data.frame(
    Section = character(0),
    Metric = character(0),
    Value = character(0),
    stringsAsFactors = FALSE
  )

  rows <- rbind(
    rows,
    data.frame(Section = "Run", Metric = "Season", Value = as.character(season), stringsAsFactors = FALSE),
    data.frame(Section = "Run", Metric = "Refreshed UTC", Value = ifelse(is.na(refreshed_at), "", refreshed_at), stringsAsFactors = FALSE),
    data.frame(Section = "Projection", Metric = "Systems Used", Value = systems, stringsAsFactors = FALSE)
  )

  if (nrow(sys_pairs) > 0) {
    for (i in seq_len(nrow(sys_pairs))) {
      rows <- rbind(rows, data.frame(
        Section = "Projection System Weights",
        Metric = toupper(sys_pairs$metric[i]),
        Value = as.character(sys_pairs$value[i]),
        stringsAsFactors = FALSE
      ))
    }
  }

  if (nrow(cat_pairs) > 0) {
    for (i in seq_len(nrow(cat_pairs))) {
      rows <- rbind(rows, data.frame(
        Section = "Stat Category Weights",
        Metric = toupper(cat_pairs$metric[i]),
        Value = as.character(cat_pairs$value[i]),
        stringsAsFactors = FALSE
      ))
    }
  }

  rows <- rbind(
    rows,
    data.frame(Section = "ADP Run", Metric = "Draft Type", Value = adp_draft_type_label(adp_draft_type), stringsAsFactors = FALSE),
    data.frame(Section = "ADP Run", Metric = "Draft Type ID", Value = ifelse(is.na(adp_draft_type), "", as.character(adp_draft_type)), stringsAsFactors = FALSE),
    data.frame(Section = "ADP Run", Metric = "Number of Teams", Value = ifelse(is.na(adp_num_teams), "", as.character(adp_num_teams)), stringsAsFactors = FALSE),
    data.frame(Section = "ADP Run", Metric = "Date From", Value = ifelse(is.na(adp_from), "", adp_from), stringsAsFactors = FALSE),
    data.frame(Section = "ADP Run", Metric = "Date To", Value = ifelse(is.na(adp_to), "", adp_to), stringsAsFactors = FALSE),
    data.frame(Section = "ADP Run", Metric = "Download Status", Value = adp_download_status, stringsAsFactors = FALSE),
    data.frame(Section = "ADP Run", Metric = "Downloaded At UTC", Value = ifelse(is.na(adp_downloaded_at), "", adp_downloaded_at), stringsAsFactors = FALSE),
    data.frame(Section = "ADP Run", Metric = "Pipeline ADP Source", Value = adp_source, stringsAsFactors = FALSE)
  )

  rows
}

refresh_meta_path <- file.path(cfg$paths$processed_dir, sprintf("%s_refresh_metadata.csv", as.integer(cfg$season)))
adp_meta_path <- file.path(cfg$paths$processed_dir, sprintf("%s_adp_download_metadata.csv", as.integer(cfg$season)))

refresh_meta <- safe_read_csv(refresh_meta_path)
adp_meta <- safe_read_csv(adp_meta_path)
rows <- build_rows(refresh_meta, adp_meta, cfg)

message("Authenticating Google Sheets...")
auth_google_sheets()

message(sprintf("Writing %s rows to tab '%s'...", nrow(rows), run_data_tab))
googlesheets4::sheet_write(rows, ss = sheet_url, sheet = run_data_tab)

# Keep Run Data tab before Projections_Bats.
if (nzchar(projection_tab)) {
  googlesheets4::sheet_relocate(sheet_url, sheet = run_data_tab, .before = projection_tab)
}

message("Run Data tab synced.")
