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
  refreshed_date <- as.Date(Sys.Date())
  systems <- tolower(paste(cfg$projection$systems, collapse = ","))
  system_weights_text <- paste(sprintf("%s=%s", names(cfg$projection$system_weights), as.numeric(cfg$projection$system_weights)), collapse = ",")
  category_weights_text <- paste(sprintf("%s=%s", names(cfg$projection$category_weights), as.numeric(cfg$projection$category_weights)), collapse = ",")

  if (!is.null(refresh_meta) && nrow(refresh_meta) > 0) {
    r <- refresh_meta[1, , drop = FALSE]
    season <- suppressWarnings(as.integer(r$season[1]))
    refreshed_at <- as.character(r$refreshed_at_utc[1])
    refreshed_date <- suppressWarnings(as.Date(substr(refreshed_at, 1, 10)))
    if (is.na(refreshed_date)) {
      refreshed_date <- as.Date(Sys.Date())
    }
    if (nzchar(as.character(r$systems[1]))) systems <- as.character(r$systems[1])
    if (nzchar(as.character(r$weights[1]))) system_weights_text <- as.character(r$weights[1])
    if (nzchar(as.character(r$category_weights[1]))) category_weights_text <- as.character(r$category_weights[1])
  }

  adp_from <- NA_character_
  adp_to <- NA_character_
  adp_draft_type <- cfg$adp$draft_type
  adp_num_teams <- cfg$adp$num_teams

  if (!is.null(refresh_meta) && nrow(refresh_meta) > 0) {
    r <- refresh_meta[1, , drop = FALSE]
    rf <- as.character(r$nfbc_adp_from_date[1])
    rt <- as.character(r$nfbc_adp_to_date[1])
    if (nzchar(rf) && !identical(rf, "NA")) adp_from <- rf
    if (nzchar(rt) && !identical(rt, "NA")) adp_to <- rt
  }

  if (!is.null(adp_meta) && nrow(adp_meta) > 0) {
    a <- adp_meta[1, , drop = FALSE]
    af <- as.character(a$from_date[1])
    at <- as.character(a$to_date[1])
    if (nzchar(af) && !identical(af, "NA")) adp_from <- af
    if (nzchar(at) && !identical(at, "NA")) adp_to <- at
    adp_draft_type <- suppressWarnings(as.integer(a$draft_type[1]))
    adp_num_teams <- suppressWarnings(as.integer(a$num_teams[1]))
  }

  # Fallback: derive configured window if exact download window is unavailable.
  if (is.na(adp_from) || !nzchar(adp_from) || identical(adp_from, "NA")) {
    adp_from <- as.character(as.Date(refreshed_date) - as.integer(cfg$adp$lookback_days))
  }
  if (is.na(adp_to) || !nzchar(adp_to) || identical(adp_to, "NA")) {
    adp_to <- as.character(refreshed_date)
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
    data.frame(Section = "ADP Run", Metric = "Number of Teams", Value = ifelse(is.na(adp_num_teams), "", as.character(adp_num_teams)), stringsAsFactors = FALSE),
    data.frame(Section = "ADP Run", Metric = "Date From", Value = ifelse(is.na(adp_from), "", adp_from), stringsAsFactors = FALSE),
    data.frame(Section = "ADP Run", Metric = "Date To", Value = ifelse(is.na(adp_to), "", adp_to), stringsAsFactors = FALSE)
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

# Keep additional scroll space beyond populated data.
googlesheets4::sheet_resize(
  ss = sheet_url,
  sheet = run_data_tab,
  nrow = max(nrow(rows) + 100L, 150L),
  ncol = max(ncol(rows) + 100L, 120L),
  exact = FALSE
)

# Keep Run Data tab before Projections_Bats.
if (nzchar(projection_tab)) {
  googlesheets4::sheet_relocate(sheet_url, sheet = run_data_tab, .before = projection_tab)
}

message("Run Data tab synced.")
