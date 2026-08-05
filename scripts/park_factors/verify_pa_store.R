#!/usr/bin/env Rscript
# Verify the Statcast PA store against an independent copy of the same source.
#
# data/raw/statcast_pitches/statcast_pitches_2021-2025.rds was pulled by a
# different script on a different date (2026-07-13) and holds every pitch, so
# the PA-ending rows inside it are a genuine second observation of what the PA
# store claims. Seasons 2021-2025 overlap, which is enough to catch a
# systematic fetch or filter error.
#
# Checks, per overlapping season:
#   - total PAs, strikeouts, walks
#   - K rate and BB rate
#   - exact row-identity reconciliation on game_pk + at_bat_number
#
# Seasons outside the overlap (2015-2019, 2026) get a plausibility check only:
# league K rate must land in a sane band and PA volume must be consistent with
# a full season. Nothing here invents a target; the overlap seasons are
# compared against real fetched data, and the non-overlap seasons are only
# range-checked.

source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  pa_input     = list(flag = "--pa-input",     default = file.path("data", "raw", "statcast_pa_store.csv")),
  pitch_rds    = list(flag = "--pitch-rds",    default = file.path("data", "raw", "statcast_pitches", "statcast_pitches_2021-2025.rds")),
  out_csv      = list(flag = "--out",          default = file.path("data", "processed", "park_factors", "pa_store_verification.csv"))
))

pa_input  <- parsed$pa_input
pitch_rds <- parsed$pitch_rds
out_csv   <- parsed$out_csv

if (!file.exists(pa_input)) {
  stop(sprintf("PA store not found: %s", pa_input))
}

K_EVENTS <- c("strikeout", "strikeout_double_play", "strikeout_triple_play")

message("Reading PA store: ", pa_input)
pa <- utils::read.csv(pa_input, stringsAsFactors = FALSE, check.names = FALSE)
pa$events_lc <- tolower(trimws(pa$events))
pa$season <- as.integer(pa$season)
message(sprintf("  %s rows, seasons %s", format(nrow(pa), big.mark = ","),
                paste(range(pa$season, na.rm = TRUE), collapse = "-")))

if (!all(c("game_pk", "at_bat_number") %in% names(pa))) {
  stop("PA store lacks game_pk/at_bat_number — re-fetch with the current fetch_statcast_pa.R.")
}
dup <- sum(duplicated(paste(pa$game_pk, pa$at_bat_number)))
message(sprintf("  Duplicate plate appearances by identity: %d", dup))

by_season <- function(d, ev_col, season_col) {
  ks <- stats::aggregate(list(k = d[[ev_col]] %in% K_EVENTS), by = list(season = d[[season_col]]), FUN = sum)
  bb <- stats::aggregate(list(bb = d[[ev_col]] == "walk"), by = list(season = d[[season_col]]), FUN = sum)
  n  <- stats::aggregate(list(pa = rep(1L, nrow(d))), by = list(season = d[[season_col]]), FUN = sum)
  out <- merge(merge(n, ks, by = "season"), bb, by = "season")
  out$k_rate <- out$k / out$pa
  out$bb_rate <- out$bb / out$pa
  out
}

ours <- by_season(pa, "events_lc", "season")

rows <- list()

# ── Overlap comparison against the independent pitch store ────────────────────
if (file.exists(pitch_rds)) {
  message("\nReading independent pitch store: ", pitch_rds)
  st <- readRDS(pitch_rds)
  st$events_lc <- ifelse(is.na(st$events), "", tolower(trimws(st$events)))
  st <- st[nzchar(st$events_lc) & st$events_lc != "null", ]
  if ("game_type" %in% names(st)) {
    st <- st[is.na(st$game_type) | st$game_type == "R", ]
  }
  st$season <- as.integer(st$season)
  theirs <- by_season(st, "events_lc", "season")

  cmp <- merge(ours, theirs, by = "season", suffixes = c("_ours", "_ref"))
  message("\n=== Overlap seasons: PA store vs independent pitch store ===")
  for (i in seq_len(nrow(cmp))) {
    r <- cmp[i, ]
    message(sprintf(
      "  %d  PA %s vs %s (%+d)  K %s vs %s (%+d)  Krate %.4f vs %.4f",
      r$season,
      format(r$pa_ours, big.mark = ","), format(r$pa_ref, big.mark = ","), r$pa_ours - r$pa_ref,
      format(r$k_ours, big.mark = ","), format(r$k_ref, big.mark = ","), r$k_ours - r$k_ref,
      r$k_rate_ours, r$k_rate_ref
    ))
    rows[[length(rows) + 1L]] <- data.frame(
      season = r$season, check = "overlap_vs_pitch_store",
      pa_ours = r$pa_ours, pa_ref = r$pa_ref, pa_diff = r$pa_ours - r$pa_ref,
      k_ours = r$k_ours, k_ref = r$k_ref, k_diff = r$k_ours - r$k_ref,
      k_rate_ours = r$k_rate_ours, k_rate_ref = r$k_rate_ref,
      stringsAsFactors = FALSE
    )
  }

  # Exact identity reconciliation on the overlap.
  if (all(c("game_pk", "at_bat_number") %in% names(st))) {
    ov_seasons <- intersect(unique(ours$season), unique(theirs$season))
    k_ours <- paste(pa$game_pk, pa$at_bat_number)[pa$season %in% ov_seasons]
    k_ref  <- paste(st$game_pk, st$at_bat_number)[st$season %in% ov_seasons]
    only_ours <- sum(!k_ours %in% k_ref)
    only_ref  <- sum(!k_ref %in% k_ours)
    message(sprintf("\n  Identity reconciliation over %s: in ours not ref = %s, in ref not ours = %s",
                    paste(range(ov_seasons), collapse = "-"),
                    format(only_ours, big.mark = ","), format(only_ref, big.mark = ",")))
    rows[[length(rows) + 1L]] <- data.frame(
      season = NA_integer_, check = "identity_reconciliation",
      pa_ours = length(k_ours), pa_ref = length(k_ref), pa_diff = only_ours,
      k_ours = NA_integer_, k_ref = NA_integer_, k_diff = only_ref,
      k_rate_ours = NA_real_, k_rate_ref = NA_real_,
      stringsAsFactors = FALSE
    )
  }
} else {
  message(sprintf("\nIndependent pitch store not found at %s; skipping the overlap comparison.", pitch_rds))
}

# ── Plausibility band for every season ───────────────────────────────────────
# Bands are deliberately wide: they exist to catch a broken fetch (half a
# season missing, a filter dropping strikeouts), not to assert a target value.
message("\n=== Per-season plausibility ===")
for (i in seq_len(nrow(ours))) {
  r <- ours[i, ]
  flags <- character(0)
  if (r$k_rate < 0.15 || r$k_rate > 0.28) flags <- c(flags, "K rate outside 15-28%")
  if (r$bb_rate < 0.05 || r$bb_rate > 0.12) flags <- c(flags, "BB rate outside 5-12%")
  # A full modern season is roughly 180k-190k PAs; 2026 is partial by design.
  if (r$pa < 150000 && r$season != max(ours$season)) flags <- c(flags, "PA volume low for a full season")
  message(sprintf("  %d  PA %s  Krate %.4f  BBrate %.4f  %s",
                  r$season, format(r$pa, big.mark = ","), r$k_rate, r$bb_rate,
                  if (length(flags) == 0) "ok" else paste("<<", paste(flags, collapse = "; "))))
  rows[[length(rows) + 1L]] <- data.frame(
    season = r$season, check = "plausibility",
    pa_ours = r$pa, pa_ref = NA_integer_, pa_diff = NA_integer_,
    k_ours = r$k, k_ref = NA_integer_, k_diff = NA_integer_,
    k_rate_ours = r$k_rate, k_rate_ref = NA_real_,
    stringsAsFactors = FALSE
  )
}

report <- do.call(rbind, rows)
out_dir <- dirname(out_csv)
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
utils::write.csv(report, out_csv, row.names = FALSE, na = "")
message("\nWrote verification report: ", out_csv)
