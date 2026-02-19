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

normalize_df_for_compare <- function(df) {
  out <- as.data.frame(df, stringsAsFactors = FALSE)
  for (nm in names(out)) {
    if (is.numeric(out[[nm]])) {
      out[[nm]] <- ifelse(is.na(out[[nm]]), NA, round(out[[nm]], 6))
    } else {
      out[[nm]] <- trimws(as.character(out[[nm]]))
      out[[nm]][out[[nm]] == ""] <- NA
    }
  }
  out
}

frames_equal <- function(a, b) {
  if (!identical(names(a), names(b))) return(FALSE)
  if (nrow(a) != nrow(b)) return(FALSE)
  identical(normalize_df_for_compare(a), normalize_df_for_compare(b))
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
