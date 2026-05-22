#!/usr/bin/env Rscript
# run_abrl_pipeline.R
# Orchestrator for the daily adjusted barrel (aBrl) refresh pipeline.
#
# Steps:
#   1. Fetch new BBE data from Savant (incremental — cached chunks are skipped)
#   2. Rebuild adjusted barrel outputs (all seasons, using updated BBE store)
#   3. Export to Google Sheets
#   4. Copy output CSVs to fbb-tools for Shiny app
#
# Usage:
#   Rscript scripts/run_abrl_pipeline.R                       # full pipeline
#   Rscript scripts/run_abrl_pipeline.R --skip-sheets         # skip Google Sheets export
#   Rscript scripts/run_abrl_pipeline.R --skip-fetch          # skip BBE fetch (use existing data)
#   Rscript scripts/run_abrl_pipeline.R --current-season-only # only fetch current season BBE

project_root <- normalizePath(file.path(dirname(
  if (interactive()) rstudioapi::getSourceEditorContext()$path
  else commandArgs(trailingOnly = FALSE) |>
    grep("--file=", x = _, value = TRUE) |>
    sub("--file=", "", x = _) |>
    normalizePath()
), ".."), mustWork = TRUE)

setwd(project_root)

args <- commandArgs(trailingOnly = TRUE)
skip_sheets        <- "--skip-sheets" %in% args
skip_fetch         <- "--skip-fetch" %in% args
current_season_only <- "--current-season-only" %in% args

current_year <- as.integer(format(Sys.Date(), "%Y"))

cat("=== aBrl Pipeline ===\n")
cat(sprintf("  Project root: %s\n", project_root))
cat(sprintf("  Current year: %d\n", current_year))
cat(sprintf("  Skip fetch:   %s\n", skip_fetch))
cat(sprintf("  Skip sheets:  %s\n", skip_sheets))
cat(sprintf("  Current season only: %s\n", current_season_only))
cat("\n")

# ── 1. Fetch BBE data ────────────────────────────────────────────────────────

if (!skip_fetch) {
  cat("── Step 1: Fetching BBE data from Savant ──\n")

  fetch_args <- c("scripts/fetch_statcast_bbe.R")

  if (current_season_only) {
    fetch_args <- c(fetch_args,
                    "--start-season", as.character(current_year),
                    "--end-season",   as.character(current_year))
  } else {
    fetch_args <- c(fetch_args,
                    "--start-season", "2015",
                    "--end-season",   as.character(current_year))
  }

  cat(sprintf("  Running: Rscript %s\n", paste(fetch_args, collapse = " ")))
  status <- system2("Rscript", fetch_args)
  if (status != 0) stop("BBE fetch failed with exit code ", status)
  cat("  BBE fetch complete.\n\n")
} else {
  cat("── Step 1: Skipping BBE fetch (--skip-fetch) ──\n\n")
}

# ── 2. Build adjusted barrels ─────────────────────────────────────────────────

cat("── Step 2: Building adjusted barrels ──\n")
status <- system2("Rscript", "scripts/build_adjusted_barrels.R")
if (status != 0) stop("Adjusted barrel build failed with exit code ", status)
cat("  Build complete.\n\n")

# ── 3. Export to Google Sheets ────────────────────────────────────────────────

if (!skip_sheets) {
  cat("── Step 3: Exporting to Google Sheets ──\n")
  status <- system2("Rscript", "scripts/export_abrl_sheets.R")
  if (status != 0) stop("Google Sheets export failed with exit code ", status)
  cat("  Sheets export complete.\n\n")
} else {
  cat("── Step 3: Skipping Sheets export (--skip-sheets) ──\n\n")
}

# ── 4. Copy outputs to fbb-tools ─────────────────────────────────────────────

cat("── Step 4: Copying outputs to fbb-tools ──\n")

src_dir  <- file.path(project_root, "data", "processed")
dest_dir <- file.path(project_root, ".claude", "worktrees", "frosty-moore",
                      "fbb-tools", "data", "processed")

if (!dir.exists(dest_dir)) {
  dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
}

abrl_files <- c(
  "adjusted_barrel_hitters.csv",
  "adjusted_barrel_metadata.csv",
  "adjusted_barrel_seasons.csv",
  "adjusted_barrel_hr_rates.csv",
  "adjusted_barrel_convergence.csv",
  "adjusted_barrel_stabilization.csv"
)

for (f in abrl_files) {
  src <- file.path(src_dir, f)
  if (file.exists(src)) {
    file.copy(src, file.path(dest_dir, f), overwrite = TRUE)
    cat(sprintf("  Copied: %s\n", f))
  } else {
    cat(sprintf("  WARNING: %s not found in %s\n", f, src_dir))
  }
}

cat("\n=== aBrl Pipeline Complete ===\n")
