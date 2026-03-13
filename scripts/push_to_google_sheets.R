#!/usr/bin/env Rscript

source(file.path("R", "pipeline_config.R"))
source(file.path("R", "gsheets_auth.R"))

parsed <- parse_cli_args(list(
  config = list(flag = "--config", default = file.path("config", "pipeline.yml"))
))
args <- parsed$positional
cfg <- load_pipeline_config(parsed$config)

sheet_url <- if (length(args) >= 1) args[[1]] else cfg$google_sheets$workbook_url
csv_path <- if (length(args) >= 2) args[[2]] else file.path(cfg$paths$processed_dir, sprintf("%s_hitters_z_scored_aggregate_projection_output.csv", cfg$season))
tab_name <- if (length(args) >= 3) args[[3]] else cfg$google_sheets$projection_tab

if (!nzchar(sheet_url)) {
  stop("Usage: Rscript scripts/push_to_google_sheets.R <google_sheet_url> [csv_path] [tab_name] [--config path]")
}

if (!file.exists(csv_path)) {
  stop(sprintf("CSV path not found: %s", csv_path))
}

if (!requireNamespace("googlesheets4", quietly = TRUE)) {
  stop("Package 'googlesheets4' is required.")
}

message("Reading local CSV...")
dat <- utils::read.csv(csv_path, stringsAsFactors = FALSE, check.names = FALSE)

message("Authenticating with Google (one-time OAuth may open browser)...")
auth_google_sheets()

existing <- tryCatch(
  as.data.frame(googlesheets4::read_sheet(sheet_url, sheet = tab_name), stringsAsFactors = FALSE),
  error = function(e) data.frame(stringsAsFactors = FALSE)
)

if (nrow(existing) > 0 && frames_equal(existing, dat)) {
  message(sprintf("No changes for tab '%s'; skipping write.", tab_name))
} else {
  message(sprintf("Writing %s rows to tab '%s'...", nrow(dat), tab_name))
  googlesheets4::sheet_write(
    data = dat,
    ss = sheet_url,
    sheet = tab_name
  )
}

message("Google Sheet update complete.")
