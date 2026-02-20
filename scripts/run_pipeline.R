#!/usr/bin/env Rscript

args_raw <- commandArgs(trailingOnly = TRUE)

config_path <- file.path("config", "pipeline.yml")
skip_adp_download <- FALSE
skip_sheets <- FALSE

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
  if (arg == "--skip-adp-download") {
    skip_adp_download <- TRUE
    i <- i + 1
    next
  }
  if (arg == "--skip-sheets") {
    skip_sheets <- TRUE
    i <- i + 1
    next
  }
  stop(sprintf("Unknown argument: %s", arg))
}

source(file.path("R", "pipeline_config.R"))
cfg <- load_pipeline_config(config_path)

run_rscript <- function(script, args = character(0), retries = 2L) {
  cmd <- c(script, args)
  quoted_cmd <- shQuote(cmd)
  for (attempt in seq_len(retries + 1L)) {
    message(sprintf("Running: Rscript %s (attempt %s/%s)", paste(cmd, collapse = " "), attempt, retries + 1L))
    status <- system2("Rscript", quoted_cmd)
    if (identical(status, 0L)) {
      return(invisible(TRUE))
    }
    if (attempt <= retries) {
      Sys.sleep(2 * attempt)
    }
  }
  stop(sprintf("Step failed after retries: Rscript %s", paste(cmd, collapse = " ")))
}

if (!skip_adp_download) {
  adp_download_ok <- TRUE
  tryCatch(
    run_rscript("scripts/fetch_nfbc_adp.R", c("--config", config_path)),
    error = function(e) {
      adp_download_ok <<- FALSE
      warning(sprintf(
        "ADP download step failed; continuing with existing local ADP file if present. Details: %s",
        conditionMessage(e)
      ))
    }
  )

  if (!adp_download_ok) {
    local_adp <- cfg$adp$local_tsv
    if (!nzchar(local_adp) || !file.exists(local_adp)) {
      stop("ADP download failed and no local ADP TSV exists to fall back on.")
    }
    message(sprintf("Using fallback local ADP TSV: %s", local_adp))
  }
}

run_rscript("scripts/fetch_projections.R", c("--config", config_path))

pitcher_refresh_ok <- TRUE
tryCatch(
  run_rscript(
    "scripts/build_pitcher_integration_table.R",
    c(
      "--config", config_path,
      "--season", as.character(cfg$season),
      "--pitcher-system", as.character(cfg$pitcher$projection$system),
      "--refresh-projections",
      "--no-sheet-export"
    )
  ),
  error = function(e) {
    pitcher_refresh_ok <<- FALSE
    warning(sprintf(
      "Pitcher integration refresh failed; continuing with existing pitcher table if present. Details: %s",
      conditionMessage(e)
    ))
  }
)

if (!pitcher_refresh_ok) {
  pitcher_out <- file.path(
    cfg$paths$processed_dir,
    sprintf("%s_pitchers_integrated_table.csv", cfg$season)
  )
  if (!file.exists(pitcher_out)) {
    stop("Pitcher integration refresh failed and no existing integrated pitcher table is available.")
  }
}

if (!skip_sheets) {
  ss <- cfg$google_sheets$workbook_url
  if (!nzchar(ss)) {
    stop("google_sheets.workbook_url is empty in config. Cannot sync sheets.")
  }

  source_ranks <- cfg$google_sheets$source_ranks_url
  if (!nzchar(source_ranks)) {
    source_ranks <- ss
  }

  season_file <- file.path(
    cfg$paths$processed_dir,
    sprintf("%s_hitters_z_scored_aggregate_projection_output.csv", cfg$season)
  )

  run_rscript("scripts/sync_run_data_tab.R", c(ss, cfg$google_sheets$run_data_tab, cfg$google_sheets$projection_tab, "--config", config_path))
  run_rscript("scripts/push_to_google_sheets.R", c(ss, season_file, cfg$google_sheets$projection_tab, "--config", config_path))
  run_rscript("scripts/sync_adp_tab.R", c(ss, cfg$google_sheets$projection_tab, cfg$google_sheets$adp_tab, "--config", config_path))
  run_rscript("scripts/sync_position_tabs.R", c(source_ranks, ss, cfg$google_sheets$projection_tab, "--config", config_path))
  run_rscript("scripts/setup_rbell_team_targets.R", c(ss, cfg$google_sheets$rbll_tab, cfg$google_sheets$projection_tab, "--config", config_path))
  run_rscript("scripts/format_workbook_tabs.R", c(ss, "--config", config_path))
}

message("Pipeline complete.")
