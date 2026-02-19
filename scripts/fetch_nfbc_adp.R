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
source(file.path("R", "fangraphs_projections.R"))

cfg <- load_pipeline_config(config_path)

output_path <- if (length(args) >= 1) args[[1]] else cfg$adp$local_tsv
draft_type <- if (length(args) >= 2) suppressWarnings(as.integer(args[[2]])) else cfg$adp$draft_type
num_teams <- if (length(args) >= 3) suppressWarnings(as.integer(args[[3]])) else cfg$adp$num_teams
lookback_days <- if (length(args) >= 4) suppressWarnings(as.integer(args[[4]])) else cfg$adp$lookback_days
to_date <- if (length(args) >= 5) as.Date(args[[5]]) else Sys.Date()
from_date <- if (length(args) >= 6) as.Date(args[[6]]) else as.Date(to_date) - as.integer(lookback_days)

if (!nzchar(output_path)) {
  stop("Output path is empty. Set adp.local_tsv in config or pass an output path.")
}

dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)

download_nfbc_adp_tsv(
  output_path = output_path,
  from_date = from_date,
  to_date = to_date,
  draft_type = draft_type,
  num_teams = num_teams
)

message(sprintf("Saved ADP TSV to %s", output_path))
message(sprintf(
  "Filters: from_date=%s to_date=%s draft_type=%s num_teams=%s",
  as.character(from_date),
  as.character(to_date),
  draft_type,
  num_teams
))
