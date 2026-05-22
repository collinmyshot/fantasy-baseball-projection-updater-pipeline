#!/usr/bin/env Rscript
# build_adjusted_barrels.R
# Season-adjusted barrel definitions via EV threshold recalibration.
#
# Tango's original barrel (2015-2016 calibration):
#   speed >= 98, angle 4-50,
#   speed*1.5 - angle >= 117, speed + angle >= 124
#   Target: zone-average wOBA matching ~1.462 (calibration-period barrel wOBA)
#
# Approach: for each season, find the EV shift (delta) applied uniformly
# to all EV terms in the barrel formula that maintains the same zone-average
# wOBA as the 2015-2016 calibration. This preserves Tango's barrel zone
# shape while adjusting for year-to-year changes in batted ball outcomes.
#
# Why EV-only (not EV+LA): tested 2-parameter shifts (EV + LA) but the
# optimizer doesn't converge to (0,0) on calibration data, indicating
# degeneracy rather than meaningful LA drift. EV-only reproduces Tango's
# original zone exactly on 2015-2016 (delta ≈ 0).

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tidyr)
  library(jsonlite)
})

project_root <- normalizePath(file.path(dirname(
  if (interactive()) rstudioapi::getSourceEditorContext()$path
  else commandArgs(trailingOnly = FALSE) |>
    grep("--file=", x = _, value = TRUE) |>
    sub("--file=", "", x = _) |>
    normalizePath()
), ".."), mustWork = TRUE)

out_dir <- file.path(project_root, "data", "processed")

# ── 1. Load BBE store ───────────────────────────────────────────────────────

bbe_path <- file.path(project_root, "data", "raw", "statcast_bbe_store.csv")
stopifnot(file.exists(bbe_path))

cat("Loading BBE store...\n")
bbe <- read_csv(bbe_path, col_types = cols(.default = col_guess()), show_col_types = FALSE)
cat(sprintf("  %d total rows\n", nrow(bbe)))

# Deduplicate: player_type=pitcher fetch creates exact duplicate rows (~25%)
bbe <- distinct(bbe)
cat(sprintf("  %d rows after deduplication\n", nrow(bbe)))

seasons <- setdiff(2015:as.integer(format(Sys.Date(), "%Y")), 2020)
bbe <- bbe |> filter(season %in% seasons, game_type == "R", !is.na(events))
cat(sprintf("  %d PA-ending BBE across %d seasons\n", nrow(bbe), length(seasons)))

# ── 2. Tango barrel definition ─────────────────────────────────────────────

is_tango_barrel <- function(ev, la) {
  !is.na(ev) & !is.na(la) &
    ev >= 98 & la >= 4 & la <= 50 &
    (ev * 1.5 - la) >= 117 &
    (ev + la) >= 124
}

is_adj_barrel <- function(ev, la, delta) {
  ev_adj <- ev + delta
  !is.na(ev) & !is.na(la) &
    ev_adj >= 98 & la >= 4 & la <= 50 &
    (ev_adj * 1.5 - la) >= 117 &
    (ev_adj + la) >= 124
}

bbe <- bbe |> mutate(
  barrel_tango = is_tango_barrel(launch_speed, launch_angle),
  is_hit = events %in% c("single", "double", "triple", "home_run"),
  total_bases = case_when(
    events == "single"   ~ 1L,
    events == "double"   ~ 2L,
    events == "triple"   ~ 3L,
    events == "home_run" ~ 4L,
    TRUE ~ 0L
  )
)
cat(sprintf("  %d Tango barrels (%.1f%% of BBE)\n",
            sum(bbe$barrel_tango), mean(bbe$barrel_tango) * 100))

# ── 3. Calibration target ──────────────────────────────────────────────────

cal_data <- bbe |> filter(season %in% c(2015, 2016), barrel_tango)
cal_woba <- mean(cal_data$woba_value, na.rm = TRUE)

cat(sprintf("\nCalibration (2015-2016): zone wOBA = %.3f, N = %d barrels\n",
            cal_woba, nrow(cal_data)))

# ── 4. Per-season EV shift calibration ─────────────────────────────────────

find_ev_shift <- function(data, target_woba, tol = 0.003) {
  lo <- -5; hi <- 5
  for (i in 1:60) {
    mid <- (lo + hi) / 2
    idx <- is_adj_barrel(data$launch_speed, data$launch_angle, mid)
    n <- sum(idx)
    if (n < 50) { hi <- mid; next }
    w <- mean(data$woba_value[idx], na.rm = TRUE)
    if (abs(w - target_woba) < tol) break
    if (w > target_woba) lo <- mid else hi <- mid
  }
  list(delta = mid, n = n, woba = w)
}

cat("\n── Per-Season Calibration ──\n")
cat(sprintf("Target zone wOBA: %.3f\n\n", cal_woba))

season_results <- lapply(seasons, function(s) {
  sdata <- bbe |> filter(season == s)
  tango_brl <- sdata |> filter(barrel_tango)
  res <- find_ev_shift(sdata, cal_woba)

  # Barrel AVG/SLG for Tango barrels
  tango_brl_avg <- mean(tango_brl$is_hit, na.rm = TRUE)
  tango_brl_slg <- mean(tango_brl$total_bases, na.rm = TRUE)

  # Barrel AVG/SLG for adjusted barrels
  adj_brl_data <- sdata |> filter(is_adj_barrel(launch_speed, launch_angle, res$delta))
  adj_brl_avg <- mean(adj_brl_data$is_hit, na.rm = TRUE)
  adj_brl_slg <- mean(adj_brl_data$total_bases, na.rm = TRUE)

  cat(sprintf("  %d: delta=%+.2f (floor=%.1f)  Tango: N=%5d wOBA=%.3f AVG=%.3f SLG=%.3f  Adj: N=%5d wOBA=%.3f AVG=%.3f SLG=%.3f\n",
              s, res$delta, 98 - res$delta,
              nrow(tango_brl), mean(tango_brl$woba_value, na.rm=TRUE), tango_brl_avg, tango_brl_slg,
              res$n, res$woba, adj_brl_avg, adj_brl_slg))

  tibble(
    season = s, ev_shift = round(res$delta, 3),
    ev_floor = round(98 - res$delta, 1),
    tango_n = nrow(tango_brl),
    tango_woba = round(mean(tango_brl$woba_value, na.rm=TRUE), 3),
    adj_n = res$n,
    adj_woba = round(res$woba, 3),
    total_bbe = nrow(sdata),
    tango_brl_pct = round(nrow(tango_brl) / nrow(sdata) * 100, 2),
    adj_brl_pct = round(res$n / nrow(sdata) * 100, 2),
    tango_brl_avg = round(tango_brl_avg, 3),
    tango_brl_slg = round(tango_brl_slg, 3),
    adj_brl_avg = round(adj_brl_avg, 3),
    adj_brl_slg = round(adj_brl_slg, 3)
  )
}) |> bind_rows()

print(season_results |> select(season, ev_shift, ev_floor, tango_n, adj_n,
                                 tango_brl_pct, adj_brl_pct, tango_woba, adj_woba),
      n = Inf, width = Inf)

# ── 5. Tag BBE with adjusted barrel status ─────────────────────────────────

bbe <- bbe |>
  left_join(season_results |> select(season, ev_shift), by = "season") |>
  mutate(barrel_adj = is_adj_barrel(launch_speed, launch_angle, ev_shift))

# ── 6. Resolve player names ───────────────────────────────────────────────

cat("\n── Resolving player names ──\n")
unique_batters <- unique(bbe$batter)
cat(sprintf("  %d unique batter IDs\n", length(unique_batters)))

resolve_player_names <- function(player_ids) {
  results <- list()
  chunks <- split(player_ids, ceiling(seq_along(player_ids) / 75))
  for (i in seq_along(chunks)) {
    ids_str <- paste(chunks[[i]], collapse = ",")
    url <- sprintf("https://statsapi.mlb.com/api/v1/people?personIds=%s&hydrate=currentTeam", ids_str)
    tryCatch({
      resp <- fromJSON(url, flatten = TRUE)
      if (!is.null(resp$people) && nrow(resp$people) > 0) {
        people <- resp$people
        results[[i]] <- tibble(
          batter = people$id,
          player_name = people$fullName,
          team = if ("currentTeam.abbreviation" %in% names(people))
            people$currentTeam.abbreviation else NA_character_
        )
      }
    }, error = function(e) {
      cat(sprintf("  Warning: chunk %d failed: %s\n", i, e$message))
    })
    if (i %% 20 == 0) cat(sprintf("  %d/%d chunks...\n", i, length(chunks)))
  }
  bind_rows(results)
}

player_lookup <- resolve_player_names(unique_batters)
cat(sprintf("  Resolved %d / %d\n", nrow(player_lookup), length(unique_batters)))

# ── 7. Pull PA from FanGraphs API ─────────────────────────────────────────

cat("\n── Fetching PA from FanGraphs ──\n")

fetch_fg_pa <- function(season) {
  url <- sprintf(
    "https://www.fangraphs.com/api/leaders/major-league/data?pos=all&stats=bat&lg=all&qual=1&season=%d&month=0&pageitems=2000&type=8",
    season
  )
  dat <- fromJSON(url, flatten = TRUE)
  if (is.null(dat$data) || nrow(dat$data) == 0) return(tibble())
  dat$data |>
    select(xMLBAMID, PA) |>
    mutate(season = season, batter = as.integer(xMLBAMID), pa = as.integer(PA)) |>
    select(batter, season, pa)
}

pa_data <- lapply(seasons, function(s) {
  cat(sprintf("  %d...\n", s))
  Sys.sleep(1)
  tryCatch(fetch_fg_pa(s), error = function(e) {
    cat(sprintf("  Warning: %d failed: %s\n", s, e$message))
    tibble()
  })
}) |> bind_rows()

cat(sprintf("  %d player-season PA records\n", nrow(pa_data)))

# ── 8. Hitter-season leaderboard with EV90 ────────────────────────────────

# No min BBE filter here — filtering happens at display time (Shiny / Sheets export)
# so the CSV contains ALL hitter-seasons with at least 1 BBE.

hitter_seasons <- bbe |>
  group_by(batter, season) |>
  summarise(
    total_bbe = n(),
    tango_barrels = sum(barrel_tango),
    adj_barrels = sum(barrel_adj),
    avg_ev = round(mean(launch_speed, na.rm = TRUE), 1),
    ev90 = round(quantile(launch_speed, 0.90, na.rm = TRUE), 1),
    avg_la = round(mean(launch_angle, na.rm = TRUE), 1),
    max_ev = round(max(launch_speed, na.rm = TRUE), 1),
    barrel_ev_mean = round(mean(launch_speed[barrel_tango], na.rm = TRUE), 1),
    .groups = "drop"
  ) |>
  mutate(
    tango_brl_pct = round(tango_barrels / total_bbe * 100, 2),
    adj_brl_pct = round(adj_barrels / total_bbe * 100, 2),
    brl_diff = round(adj_brl_pct - tango_brl_pct, 2),
    brl_diff_abs = adj_barrels - tango_barrels,
    pct_lost = round(ifelse(tango_barrels > 0,
                            (tango_barrels - adj_barrels) / tango_barrels * 100, 0), 1)
  ) |>
  left_join(player_lookup, by = "batter") |>
  left_join(pa_data, by = c("batter", "season"))

cat(sprintf("\n  %d hitter-seasons total\n", nrow(hitter_seasons)))

# Correlations with hitter profile (use 150 BBE min for meaningful analysis)
hs_qual <- hitter_seasons |> filter(total_bbe >= 150)
cat(sprintf("\nCorrelation of barrel rate change (adj - tango) with (N=%d, BBE>=150):\n", nrow(hs_qual)))
cat(sprintf("  Avg EV:  r = %.3f\n", cor(hs_qual$brl_diff, hs_qual$avg_ev, use="complete.obs")))
cat(sprintf("  EV90:    r = %.3f\n", cor(hs_qual$brl_diff, hs_qual$ev90, use="complete.obs")))
cat(sprintf("  Avg LA:  r = %.3f\n", cor(hs_qual$brl_diff, hs_qual$avg_la, use="complete.obs")))
cat(sprintf("  Max EV:  r = %.3f\n", cor(hs_qual$brl_diff, hs_qual$max_ev, use="complete.obs")))

# Same but only for seasons where adjustment is meaningful (post-2020)
post2020 <- hs_qual |> filter(season >= 2021)
cat(sprintf("\nPost-2020 only (%d hitter-seasons):\n", nrow(post2020)))
cat(sprintf("  Avg EV:  r = %.3f\n", cor(post2020$brl_diff, post2020$avg_ev, use="complete.obs")))
cat(sprintf("  EV90:    r = %.3f\n", cor(post2020$brl_diff, post2020$ev90, use="complete.obs")))
cat(sprintf("  Avg LA:  r = %.3f\n", cor(post2020$brl_diff, post2020$avg_la, use="complete.obs")))
cat(sprintf("  Max EV:  r = %.3f\n", cor(post2020$brl_diff, post2020$max_ev, use="complete.obs")))

cat("\n── Top 20 Losers (biggest barrel rate decrease from adjustment) ──\n")
hs_qual |>
  arrange(brl_diff) |>
  select(player_name, team, season, total_bbe, tango_brl_pct, adj_brl_pct,
         brl_diff, avg_ev, ev90) |>
  head(20) |> print(width = Inf)

cat("\n── Top 20 Gainers ──\n")
hs_qual |>
  arrange(desc(brl_diff)) |>
  select(player_name, team, season, total_bbe, tango_brl_pct, adj_brl_pct,
         brl_diff, avg_ev, ev90) |>
  head(20) |> print(width = Inf)

# ── 8. Stabilization: split-half reliability ───────────────────────────────

cat("\n── Player-Level Stabilization (Split-Half) ──\n")

stab_results <- lapply(seq(50, 400, by = 25), function(min_n) {
  eligible <- bbe |>
    group_by(batter, season) |>
    filter(n() >= min_n * 2) |>
    mutate(half = ifelse(row_number() <= n() / 2, "first", "second")) |>
    ungroup()
  if (nrow(eligible) == 0) return(NULL)
  halves <- eligible |>
    group_by(batter, season, half) |>
    summarise(
      tango_pct = mean(barrel_tango) * 100,
      adj_pct = mean(barrel_adj) * 100,
      .groups = "drop"
    ) |>
    pivot_wider(names_from = half, values_from = c(tango_pct, adj_pct))
  if (nrow(halves) < 20) return(NULL)
  tibble(
    min_bbe_per_half = min_n,
    n_hitter_seasons = nrow(halves),
    r_tango = round(cor(halves$tango_pct_first, halves$tango_pct_second, use="complete.obs"), 3),
    r_adj = round(cor(halves$adj_pct_first, halves$adj_pct_second, use="complete.obs"), 3)
  )
}) |> bind_rows()

print(stab_results, n = Inf)

# ── 9. Season-level convergence ────────────────────────────────────────────

cat("\n── Season-Level Parameter Convergence ──\n")
cat("How many BBE into a season before the EV shift stabilizes?\n\n")

convergence_results <- lapply(seasons, function(s) {
  sdata <- bbe |> filter(season == s) |> arrange(game_date)
  n_total <- nrow(sdata)
  full_res <- find_ev_shift(sdata, cal_woba)

  lapply(seq(0.1, 0.9, by = 0.1), function(f) {
    partial <- sdata |> slice_head(n = round(n_total * f))
    partial_res <- tryCatch(find_ev_shift(partial, cal_woba),
                            error = function(e) list(delta = NA_real_))
    tibble(
      season = s, fraction = f, n_bbe = nrow(partial),
      ev_shift_partial = round(partial_res$delta, 3),
      ev_shift_full = round(full_res$delta, 3),
      error = round(partial_res$delta - full_res$delta, 3)
    )
  }) |> bind_rows()
}) |> bind_rows()

convergence_summary <- convergence_results |>
  group_by(fraction) |>
  summarise(
    avg_n_bbe = round(mean(n_bbe)),
    mean_abs_error = round(mean(abs(error), na.rm = TRUE), 3),
    max_abs_error = round(max(abs(error), na.rm = TRUE), 3),
    sd_error = round(sd(error, na.rm = TRUE), 3),
    .groups = "drop"
  )

cat("── Convergence Summary ──\n")
print(convergence_summary, n = Inf, width = Inf)

# Translate fractions to approximate calendar dates
cat("\n── Approximate Calendar Mapping ──\n")
cat("(Based on ~165K BBE per season, April 1 through Sept 30 = ~183 days)\n")
for (i in seq_len(nrow(convergence_summary))) {
  f <- convergence_summary$fraction[i]
  day_approx <- round(f * 183)
  month_day <- as.Date("2025-04-01") + day_approx
  cat(sprintf("  %.0f%% (%s, ~%.0fK BBE): avg error = %.3f mph, max = %.3f mph\n",
              f * 100, format(month_day, "%b %d"), convergence_summary$avg_n_bbe[i] / 1000,
              convergence_summary$mean_abs_error[i], convergence_summary$max_abs_error[i]))
}

# ── 10. HR/Barrel rates per season ────────────────────────────────────────
# Uses PA-ending BBEs only (events not NA). Computes barrel_hr / barrels
# ("earned" HR rate) — excludes foul balls with barrel-qualifying EV/LA.

cat("\n── HR/Barrel Rates ──\n")
hr_rates <- bbe |>
  group_by(season) |>
  summarise(
    tango_brls = sum(barrel_tango),
    tango_hr = sum(barrel_tango & events == "home_run"),
    adj_brls = sum(barrel_adj),
    adj_hr = sum(barrel_adj & events == "home_run"),
    .groups = "drop"
  ) |>
  mutate(
    tango_hr_brl_pct = round(tango_hr / tango_brls * 100, 1),
    adj_hr_brl_pct = round(adj_hr / adj_brls * 100, 1)
  )
print(hr_rates, n = Inf, width = Inf)

# ── 11. Season metadata (games played + % to stabilization) ───────────────

cat("\n── Season Metadata ──\n")
season_meta <- bbe |>
  group_by(season) |>
  summarise(games_played = n_distinct(game_date), .groups = "drop") |>
  mutate(
    # Stabilization threshold: ~115 game-dates (convergence shows <0.2 mph by ~70%)
    stability_games = 115L,
    pct_stability = pmin(round(games_played / stability_games * 100, 0), 100)
  )
print(season_meta, n = Inf, width = Inf)

# ── 12. Save outputs ──────────────────────────────────────────────────────

write_csv(season_results, file.path(out_dir, "adjusted_barrel_seasons.csv"))
write_csv(hitter_seasons, file.path(out_dir, "adjusted_barrel_hitters.csv"))
write_csv(stab_results, file.path(out_dir, "adjusted_barrel_stabilization.csv"))
write_csv(convergence_results, file.path(out_dir, "adjusted_barrel_convergence.csv"))
write_csv(hr_rates, file.path(out_dir, "adjusted_barrel_hr_rates.csv"))
write_csv(season_meta, file.path(out_dir, "adjusted_barrel_metadata.csv"))

cat(sprintf("\nWrote outputs to %s:\n", out_dir))
cat("  adjusted_barrel_seasons.csv       (per-season EV shift calibration)\n")
cat("  adjusted_barrel_hitters.csv       (hitter-season leaderboard w/ EV90)\n")
cat("  adjusted_barrel_stabilization.csv (player-level split-half reliability)\n")
cat("  adjusted_barrel_convergence.csv   (season-level parameter convergence)\n")
cat("  adjusted_barrel_hr_rates.csv      (HR/barrel rates per season)\n")
cat("  adjusted_barrel_metadata.csv      (games played + % to stabilization)\n")
cat("\nDone.\n")
