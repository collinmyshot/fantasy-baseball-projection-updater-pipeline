#!/usr/bin/env Rscript
# build_bat_speed_research.R
# Fetches bat tracking leaderboard from Savant (2024-2025),
# joins with HR/EV player-level data, outputs analysis dataset.

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
})

project_root <- normalizePath(file.path(dirname(
  if (interactive()) rstudioapi::getSourceEditorContext()$path
  else commandArgs(trailingOnly = FALSE) |>
    grep("--file=", x = _, value = TRUE) |>
    sub("--file=", "", x = _) |>
    normalizePath()
), "..", ".."), mustWork = TRUE)

out_dir <- file.path(project_root, "data", "processed")

# ── 1. Fetch bat tracking leaderboard from Savant ────────────────────────────

fetch_bat_tracking <- function(season) {
  url <- sprintf(
    paste0(
      "https://baseballsavant.mlb.com/leaderboard/bat-tracking",
      "?attackZone=&batSide=&contactType=&count=&dateStart=&dateEnd=",
      "&gameType=&isHardHit=&minSwings=0&playerType=Batter",
      "&season=%d&sortColumn=avg_bat_speed&sortDirection=desc&swingType=&csv=true"
    ),
    season
  )
  cat(sprintf("  Fetching %d bat tracking... ", season))
  dat <- tryCatch(
    read.csv(url, stringsAsFactors = FALSE, check.names = FALSE),
    error = function(e) {
      warning(sprintf("Failed for %d: %s", season, conditionMessage(e)))
      return(NULL)
    }
  )
  if (is.null(dat) || nrow(dat) == 0) return(NULL)
  names(dat) <- sub("^﻿", "", names(dat))
  dat$season <- season
  cat(sprintf("%d rows\n", nrow(dat)))
  dat
}

cat("Fetching bat tracking leaderboards...\n")
bt_list <- list()
for (yr in c(2024, 2025)) {
  bt_list[[as.character(yr)]] <- fetch_bat_tracking(yr)
  Sys.sleep(1)
}
bt_raw <- bind_rows(bt_list)
cat(sprintf("  %d total player-seasons from bat tracking\n", nrow(bt_raw)))

bt <- bt_raw |>
  transmute(
    batter      = as.integer(id),
    season      = as.integer(season),
    name_bt     = name,
    swings      = as.integer(swings_competitive),
    avg_bat_speed = as.double(avg_bat_speed),
    hard_swing_rate = as.double(hard_swing_rate),
    swing_length = as.double(swing_length)
  ) |>
  filter(!is.na(batter), !is.na(avg_bat_speed))

cat(sprintf("  %d player-seasons with valid bat speed\n", nrow(bt)))

# ── 2. Load HR/EV player-level data and join ─────────────────────────────────

hr_ev_path <- file.path(out_dir, "hr_ev_research.csv")
stopifnot(file.exists(hr_ev_path))

hr_ev <- read_csv(hr_ev_path, col_types = cols(.default = col_guess())) |>
  filter(season %in% c(2024, 2025))

cat(sprintf("  %d player-seasons in HR/EV data (2024-2025)\n", nrow(hr_ev)))

merged <- hr_ev |>
  inner_join(bt, by = c("batter", "season")) |>
  select(batter, season, name, team, pa, hr, bbe_count, max_ev, ev50, ev90,
         swings, avg_bat_speed, hard_swing_rate, swing_length)

cat(sprintf("  %d matched player-seasons\n", nrow(merged)))

write_csv(merged, file.path(out_dir, "bat_speed_research.csv"))
cat(sprintf("Wrote %s\n", file.path(out_dir, "bat_speed_research.csv")))

# ── 3. Correlations ──────────────────────────────────────────────────────────

qualified <- merged |> filter(pa >= 502) |>
  mutate(hr_per_600 = hr / pa * 600)
cat(sprintf("\nCorrelations (qualified, n=%d):\n", nrow(qualified)))

cors <- data.frame(
  metric = c("Max EV", "EV90", "EV50", "HR/600"),
  r = c(
    cor(qualified$avg_bat_speed, qualified$max_ev),
    cor(qualified$avg_bat_speed, qualified$ev90),
    cor(qualified$avg_bat_speed, qualified$ev50),
    cor(qualified$avg_bat_speed, qualified$hr_per_600)
  ),
  stringsAsFactors = FALSE
)
cors$r_sq <- cors$r^2
cors$r    <- round(cors$r, 3)
cors$r_sq <- round(cors$r_sq, 3)

print(cors)

write_csv(cors, file.path(out_dir, "bat_speed_correlations.csv"))

# ── 4. Quick bucket summaries ────────────────────────────────────────────────

cat("\n── Avg Bat Speed by HR/600 Bucket (qualified) ──\n")
qualified |>
  mutate(hr_bucket = cut(hr_per_600,
    breaks = c(-Inf, 10, 15, 20, 25, 30, 40, Inf),
    labels = c("<=10", "10-15", "15-20", "20-25", "25-30", "30-40", "40+"),
    right = TRUE)) |>
  group_by(hr_bucket) |>
  summarise(n = n(), avg_bs = round(mean(avg_bat_speed), 1),
            avg_hr600 = round(mean(hr_per_600), 1), .groups = "drop") |>
  print(n = Inf)

cat("\n── Avg Bat Speed by Max EV Bucket (qualified) ──\n")
qualified |>
  mutate(ev_bucket = cut(max_ev,
    breaks = c(-Inf, 109, 112, 115, Inf),
    labels = c("<=109", "110-112", "112-115", "115+"),
    right = TRUE)) |>
  group_by(ev_bucket) |>
  summarise(n = n(), avg_bs = round(mean(avg_bat_speed), 1),
            avg_ev = round(mean(max_ev), 1), .groups = "drop") |>
  print(n = Inf)

cat("\nDone.\n")
