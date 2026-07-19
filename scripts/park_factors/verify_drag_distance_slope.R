#!/usr/bin/env Rscript
# Verify the fitted drag/distance relationship against Savant's published rule
# of thumb: a 0.01 decrease in drag coefficient adds ~5 feet to a batted ball
# hit at 100 mph. The Carry model's drag_c slope averages over ALL air balls,
# so this script fits the same slope on the rule's own turf: HR-zone flies
# (EV 98-102 mph, LA 25-35 degrees), controlling for EV and LA.

source(file.path("R", "utils.R"))
source(file.path("R", "park_factors.R"))

parsed <- parse_cli_args(list(
  bbe_input  = list(flag = "--bbe-input",  default = file.path("data", "raw", "statcast_bbe_store.csv")),
  drag_input = list(flag = "--drag-input", default = file.path("data", "processed", "drag_daily.csv")),
  max_date   = list(flag = "--max-date",   default = "2026-06-30")
))

message("Reading store (this takes a minute)...")
raw <- utils::read.csv(parsed$bbe_input, stringsAsFactors = FALSE, check.names = FALSE)
std <- standardize_bbe_columns(raw)
std <- std[!is.na(std$game_date), ]
std$season <- as.integer(format(std$game_date, "%Y"))
std <- std[std$season != 2020, ]
max_date <- suppressWarnings(as.Date(parsed$max_date))
if (!is.na(max_date)) {
  std <- std[std$game_date <= max_date, ]
}

drag <- utils::read.csv(parsed$drag_input, stringsAsFactors = FALSE, check.names = FALSE)
drag$game_date <- as.Date(drag$game_date)
std <- merge(std, drag[, c("game_date", "mean_cd")], by = "game_date")

zone <- std[
  is.finite(std$hit_distance) & is.finite(std$launch_speed) & is.finite(std$launch_angle) &
    std$launch_speed >= 98 & std$launch_speed <= 102 &
    std$launch_angle >= 25 & std$launch_angle <= 35 &
    is.finite(std$mean_cd),
]

message(sprintf("HR-zone flies with distance + drag: %s", nrow(zone)))

fit <- stats::lm(hit_distance ~ mean_cd + launch_speed + launch_angle + I(launch_angle^2), data = zone)
slope <- stats::coef(fit)[["mean_cd"]]
se <- summary(fit)$coefficients["mean_cd", "Std. Error"]

message(sprintf("Distance slope on drag: %.0f ft per unit Cd (SE %.0f)", slope, se))
message(sprintf("Per 0.01 Cd: %+.1f ft (Savant rule of thumb: ~5 ft at 100 mph)", slope * 0.01))
