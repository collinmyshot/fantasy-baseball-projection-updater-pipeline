#!/usr/bin/env Rscript
source(file.path("R", "pipeline_config.R"))

parsed <- parse_cli_args(list(
  config      = list(flag = "--config", default = file.path("config", "pipeline.yml")),
  season_arg  = list(flag = "--season", default = ""),
  output_path = list(flag = "--output", default = "")
))

config_path     <- parsed$config
season_arg      <- parsed$season_arg
output_path_arg <- parsed$output_path
source(file.path("R", "fangraphs_projections.R"))

cfg <- load_pipeline_config(config_path)
season <- if (nzchar(season_arg)) as.integer(season_arg) else as.integer(cfg$season)

hitter_path <- file.path(cfg$paths$processed_dir, sprintf("%s_hitters_z_scored_aggregate_projection_output.csv", season))
pitcher_path <- file.path(cfg$paths$processed_dir, sprintf("%s_pitchers_integrated_table.csv", season))
output_path <- if (nzchar(output_path_arg)) {
  output_path_arg
} else {
  file.path(cfg$paths$processed_dir, sprintf("%s_ottoneu_fg_points_projections.csv", season))
}

if (!file.exists(hitter_path)) {
  stop(sprintf("Missing hitter projection file: %s", hitter_path))
}
if (!file.exists(pitcher_path)) {
  stop(sprintf("Missing pitcher projection file: %s", pitcher_path))
}

read_csv_safe <- function(path) {
  out <- tryCatch(
    utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE),
    error = function(e) NULL
  )
  if (is.null(out)) {
    stop(sprintf("Failed reading CSV: %s", path))
  }
  out
}

col_num <- function(df, choices) {
  hit <- intersect(choices, names(df))
  if (length(hit) == 0) {
    return(rep(NA_real_, nrow(df)))
  }
  suppressWarnings(as.numeric(df[[hit[1]]]))
}

col_chr <- function(df, choices) {
  hit <- intersect(choices, names(df))
  if (length(hit) == 0) {
    return(rep("", nrow(df)))
  }
  as.character(df[[hit[1]]])
}

hitters <- read_csv_safe(hitter_path)
pitchers <- read_csv_safe(pitcher_path)

hitter_points <- col_num(hitters, c("ottoneu_fg_pts"))
need_hitter_points <- is.na(hitter_points)
if (any(need_hitter_points)) {
  hitter_points <- (
    OTTONEU_FG_HITTING_POINTS[["ab"]] * score_num(col_num(hitters, c("ab", "AB"))) +
      OTTONEU_FG_HITTING_POINTS[["h"]] * score_num(col_num(hitters, c("h", "H"))) +
      OTTONEU_FG_HITTING_POINTS[["x2b"]] * score_num(col_num(hitters, c("2b", "x2b", "X2B"))) +
      OTTONEU_FG_HITTING_POINTS[["x3b"]] * score_num(col_num(hitters, c("3b", "x3b", "X3B"))) +
      OTTONEU_FG_HITTING_POINTS[["hr"]] * score_num(col_num(hitters, c("hr", "HR"))) +
      OTTONEU_FG_HITTING_POINTS[["bb"]] * score_num(col_num(hitters, c("bb", "BB"))) +
      OTTONEU_FG_HITTING_POINTS[["hbp"]] * score_num(col_num(hitters, c("hbp", "HBP"))) +
      OTTONEU_FG_HITTING_POINTS[["sb"]] * score_num(col_num(hitters, c("sb", "SB"))) +
      OTTONEU_FG_HITTING_POINTS[["cs"]] * score_num(col_num(hitters, c("cs", "CS")))
  )
}

pitcher_points <- col_num(pitchers, c("ottoneu_fg_pts"))
need_pitcher_points <- is.na(pitcher_points)
if (any(need_pitcher_points)) {
  pitcher_points <- (
    OTTONEU_FG_PITCHING_POINTS[["ip"]] * score_num(col_num(pitchers, c("proj_ip", "ip", "IP"))) +
      OTTONEU_FG_PITCHING_POINTS[["k"]] * score_num(col_num(pitchers, c("proj_k", "k", "K"))) +
      OTTONEU_FG_PITCHING_POINTS[["h"]] * score_num(col_num(pitchers, c("proj_h", "h", "H"))) +
      OTTONEU_FG_PITCHING_POINTS[["bb"]] * score_num(col_num(pitchers, c("proj_bb", "bb", "BB"))) +
      OTTONEU_FG_PITCHING_POINTS[["hbp"]] * score_num(col_num(pitchers, c("proj_hbp", "hbp", "HBP"))) +
      OTTONEU_FG_PITCHING_POINTS[["hr"]] * score_num(col_num(pitchers, c("proj_hr", "hr", "HR"))) +
      OTTONEU_FG_PITCHING_POINTS[["sv"]] * score_num(col_num(pitchers, c("proj_sv", "sv", "SV"))) +
      OTTONEU_FG_PITCHING_POINTS[["hld"]] * score_num(col_num(pitchers, c("proj_hld", "hld", "HLD", "holds", "HOLDS")))
  )
}

hitter_out <- data.frame(
  player_type = rep("hitter", nrow(hitters)),
  player_name = col_chr(hitters, c("player_name", "name")),
  team = col_chr(hitters, c("team")),
  position = col_chr(hitters, c("position")),
  adp = col_num(hitters, c("adp", "ADP")),
  pa = col_num(hitters, c("pa", "PA")),
  ab = col_num(hitters, c("ab", "AB")),
  h = col_num(hitters, c("h", "H")),
  `2b` = col_num(hitters, c("2b", "x2b", "X2B")),
  `3b` = col_num(hitters, c("3b", "x3b", "X3B")),
  bb = col_num(hitters, c("bb", "BB")),
  hbp = col_num(hitters, c("hbp", "HBP")),
  hr = col_num(hitters, c("hr", "HR")),
  sb = col_num(hitters, c("sb", "SB")),
  cs = col_num(hitters, c("cs", "CS")),
  r = col_num(hitters, c("r", "R")),
  rbi = col_num(hitters, c("rbi", "RBI")),
  ip = rep(NA_real_, nrow(hitters)),
  k = rep(NA_real_, nrow(hitters)),
  sv = rep(NA_real_, nrow(hitters)),
  hld = rep(NA_real_, nrow(hitters)),
  ottoneu_fg_pts = round(hitter_points, 1),
  stringsAsFactors = FALSE,
  check.names = FALSE
)

pitcher_out <- data.frame(
  player_type = rep("pitcher", nrow(pitchers)),
  player_name = col_chr(pitchers, c("player_name", "name")),
  team = col_chr(pitchers, c("team")),
  position = rep("P", nrow(pitchers)),
  adp = col_num(pitchers, c("adp", "ADP")),
  pa = rep(NA_real_, nrow(pitchers)),
  ab = rep(NA_real_, nrow(pitchers)),
  h = col_num(pitchers, c("proj_h", "h", "H")),
  `2b` = rep(NA_real_, nrow(pitchers)),
  `3b` = rep(NA_real_, nrow(pitchers)),
  bb = col_num(pitchers, c("proj_bb", "bb", "BB")),
  hbp = col_num(pitchers, c("proj_hbp", "hbp", "HBP")),
  hr = col_num(pitchers, c("proj_hr", "hr", "HR")),
  sb = rep(NA_real_, nrow(pitchers)),
  cs = rep(NA_real_, nrow(pitchers)),
  r = rep(NA_real_, nrow(pitchers)),
  rbi = rep(NA_real_, nrow(pitchers)),
  ip = col_num(pitchers, c("proj_ip", "ip", "IP")),
  k = col_num(pitchers, c("proj_k", "k", "K")),
  sv = col_num(pitchers, c("proj_sv", "sv", "SV")),
  hld = col_num(pitchers, c("proj_hld", "hld", "HLD", "holds", "HOLDS")),
  ottoneu_fg_pts = round(pitcher_points, 1),
  stringsAsFactors = FALSE,
  check.names = FALSE
)

out <- rbind(hitter_out, pitcher_out)
ord <- order(-out$ottoneu_fg_pts, out$adp, out$player_name, na.last = TRUE)
out <- out[ord, , drop = FALSE]
rownames(out) <- NULL

dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
utils::write.csv(out, output_path, row.names = FALSE, na = "")

message(sprintf("Wrote Ottoneu points projections: %s", output_path))
message(sprintf("Rows: %s (hitters=%s, pitchers=%s)", nrow(out), nrow(hitter_out), nrow(pitcher_out)))
