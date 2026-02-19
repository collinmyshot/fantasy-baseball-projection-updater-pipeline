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

target_sheet <- if (length(args) >= 1) args[[1]] else cfg$google_sheets$workbook_url
projection_tab <- if (length(args) >= 2) args[[2]] else cfg$google_sheets$projection_tab
adp_tab <- if (length(args) >= 3) args[[3]] else cfg$google_sheets$adp_tab

if (!nzchar(target_sheet)) {
  stop("Usage: Rscript scripts/sync_adp_tab.R <target_sheet_url> [projection_tab] [adp_tab] [--config path]")
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

message("Authenticating Google Sheets...")
auth_google_sheets()

tabs <- googlesheets4::sheet_names(target_sheet)
if (!projection_tab %in% tabs) {
  stop(sprintf("Projection tab '%s' not found.", projection_tab))
}

message("Reading projection data...")
proj <- as.data.frame(
  googlesheets4::read_sheet(target_sheet, sheet = projection_tab),
  stringsAsFactors = FALSE
)

required_cols <- c("player_name", "position", "adp", "dollars_adp")
missing <- setdiff(required_cols, names(proj))
if (length(missing) > 0) {
  stop(sprintf("Projection tab missing columns: %s", paste(missing, collapse = ", ")))
}

out <- data.frame(
  Player = as.character(proj$player_name),
  Positions = as.character(proj$position),
  ADP = suppressWarnings(as.numeric(proj$adp)),
  ADP_Dollars = suppressWarnings(as.numeric(proj$dollars_adp)),
  stringsAsFactors = FALSE
)

out <- out[order(out$ADP, out$Player, na.last = TRUE), , drop = FALSE]
rownames(out) <- NULL

existing <- tryCatch(
  as.data.frame(googlesheets4::read_sheet(target_sheet, sheet = adp_tab), stringsAsFactors = FALSE),
  error = function(e) data.frame(stringsAsFactors = FALSE)
)

if (nrow(existing) > 0 && frames_equal(existing, out)) {
  message(sprintf("No changes for tab '%s'; skipping write.", adp_tab))
} else {
  message(sprintf("Writing %s rows to tab '%s'...", nrow(out), adp_tab))
  googlesheets4::sheet_write(out, ss = target_sheet, sheet = adp_tab)
}

# Keep ADP immediately after Projections_Bats in tab order.
googlesheets4::sheet_relocate(target_sheet, sheet = adp_tab, .after = projection_tab)

message("ADP tab synced.")
