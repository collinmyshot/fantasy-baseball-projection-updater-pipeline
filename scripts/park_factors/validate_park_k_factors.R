#!/usr/bin/env Rscript
# Validation + falsification report for the park K% lens.
#
# Four questions, in the order they should be trusted:
#   1. Drag: reported as a magnitude, NOT as a pass/fail falsification.
#      An earlier version of this script treated a non-zero drag slope as a
#      red flag on the reasoning that "carry cannot move a strikeout". That
#      reasoning was wrong. Savant's Cd is estimated from four-seam fastball
#      tracking and describes the ball's drag in flight, which governs how
#      much the PITCH decelerates on its way to the plate just as much as how
#      far a batted ball carries. A draggier ball arriving slower plausibly
#      generates fewer whiffs, so a small negative slope is physics, not
#      contamination. Drag belongs in the model as a league-wide daily control
#      precisely so the park terms are estimated net of ball-environment
#      drift. What matters is that the magnitude stays small next to the park
#      spread, which is what gets reported.
#   2. Falsification: is the BB park effect ~ 0? Park physics should not move
#      walks. A BB spread comparable to the K spread would mean the model is
#      picking up something structural (umpire assignment, framing, scorer
#      behavior) rather than a park property.
#   3. Falsification: do wall-distance era events move K%? Moving a fence in
#      cannot change a strikeout, so era splits defined by dimension changes
#      should show near-zero K deltas between eras of the same venue.
#   4. External agreement: how do our park K effects compare with Savant's
#      published index_so? Savant's index is a raw ratio that does not control
#      for which batters and pitchers appeared, so agreement is expected to be
#      positive but well short of 1, and OUR spread should be materially
#      narrower. R^2 is reported (variance explained), not Pearson r.

source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  output_dir   = list(flag = "--output-dir",   default = file.path("data", "processed", "park_factors")),
  savant_csv   = list(flag = "--savant-csv",   default = file.path("data", "raw", "savant_park_factors_2025.csv")),
  events_csv   = list(flag = "--events-csv",   default = file.path("data", "manual", "park_era_events.csv")),
  report_path  = list(flag = "--report",       default = file.path("data", "processed", "park_factors", "k_validation_report.csv"))
))

output_dir  <- parsed$output_dir
savant_csv  <- parsed$savant_csv
events_csv  <- parsed$events_csv
report_path <- parsed$report_path

k_overall_path <- file.path(output_dir, "park_factors_k_overall.csv")
bb_overall_path <- file.path(output_dir, "park_factors_bb_overall.csv")
fixef_path <- file.path(output_dir, "k_model_fixed_effects.csv")
meta_path <- file.path(output_dir, "k_model_run_metadata.csv")

for (p in c(k_overall_path, fixef_path, meta_path)) {
  if (!file.exists(p)) {
    stop(sprintf("Missing K model output: %s (run scripts/park_factors/build_park_k_factors.R first)", p))
  }
}

k <- utils::read.csv(k_overall_path, stringsAsFactors = FALSE, check.names = FALSE)
fixef <- utils::read.csv(fixef_path, stringsAsFactors = FALSE, check.names = FALSE)
meta <- utils::read.csv(meta_path, stringsAsFactors = FALSE, check.names = FALSE)
meta_val <- function(key) {
  v <- meta$value[meta$key == key]
  if (length(v) == 0) NA_character_ else as.character(v[1])
}
baseline_k <- suppressWarnings(as.numeric(meta_val("baseline_k_rate")))

rows <- list()
add_row <- function(check, metric, value, note) {
  rows[[length(rows) + 1L]] <<- data.frame(
    check = check, metric = metric, value = as.character(value), note = note,
    stringsAsFactors = FALSE
  )
}

message("=== Park K% validation ===")
message(sprintf("Baseline K per PA: %.4f | park eras: %d | PAs modeled: %s",
                baseline_k, nrow(k), meta_val("rows_modeled")))

# ── 1. Drag falsification ─────────────────────────────────────────────────────
drag_k <- fixef[fixef$model == "k_per_pa" & fixef$term == "drag_c", , drop = FALSE]
if (nrow(drag_k) > 0) {
  est <- drag_k$estimate[1]
  # Savant's rule of thumb: -0.01 Cd ~ +5 ft carry. Express the K slope over
  # the same 0.01 Cd step so it can be read next to that.
  per_01 <- est * 0.01
  tval <- if ("t_value" %in% names(drag_k)) drag_k$t_value[1] else NA_real_
  message(sprintf("\n[1] Drag (control, not falsification): K slope per +0.01 Cd = %+.5f K/PA (%.3f K%% points), t = %.2f",
                  per_01, 100 * per_01, tval))

  # The question is scale, not significance: with ~2M plate appearances even a
  # trivial slope clears |t| = 2, so compare the drag effect against the park
  # spread it could plausibly distort.
  k_sd_pct <- if (file.exists(bb_overall_path)) NA_real_ else NA_real_
  ksd <- stats::sd(k$park_effect)
  ratio_to_park_sd <- abs(per_01) / ksd
  message(sprintf("    Size check: %.3f K%% points per 0.01 Cd vs park effect SD of %.3f K%% points (%.1f%% as large)",
                  100 * per_01, 100 * ksd, 100 * ratio_to_park_sd))
  verdict <- if (ratio_to_park_sd < 0.5) "SMALL relative to park spread — drag is doing its job as a control" else "LARGE relative to park spread — inspect before trusting park terms"
  message(sprintf("    Verdict: %s", verdict))
  add_row("drag_control", "k_per_pa_per_0.01_cd", sprintf("%.6f", per_01),
          "Cd governs pitch deceleration as well as carry, so a small slope is expected physics")
  add_row("drag_control", "drag_t_value", sprintf("%.3f", tval), "Significance is near-automatic at this sample size")
  add_row("drag_control", "drag_effect_vs_park_sd", sprintf("%.3f", ratio_to_park_sd), verdict)

  # A drag slope is only interpretable where drag actually varies. 2015 has no
  # daily Cd at all, so a build weighted toward early seasons can show a slope
  # that is really a season-level artifact.
  cov_share <- suppressWarnings(as.numeric(meta_val("drag_coverage_share")))
  if (is.finite(cov_share)) {
    message(sprintf("    Drag coverage in the modeled PAs: %.1f%%%s", 100 * cov_share,
                    if (cov_share < 0.75) "  (LOW — treat the slope as weakly identified)" else ""))
    add_row("drag_falsification", "drag_coverage_share", sprintf("%.4f", cov_share),
            "Slope is weakly identified when coverage is low")
  }
} else {
  message("\n[1] Drag falsification: drag_c not in the K model fixed effects.")
  add_row("drag_falsification", "k_per_pa_per_0.01_cd", NA, "drag_c absent from model")
}

# ── 2. BB sidecar ─────────────────────────────────────────────────────────────
if (file.exists(bb_overall_path)) {
  bb <- utils::read.csv(bb_overall_path, stringsAsFactors = FALSE, check.names = FALSE)
  k_sd <- stats::sd(k$park_effect)
  bb_sd <- stats::sd(bb$park_effect)
  message(sprintf("\n[2] BB falsification: park effect SD  K = %.5f (%.2f K%% pts)  BB = %.5f (%.2f BB%% pts)  ratio K/BB = %.2f",
                  k_sd, 100 * k_sd, bb_sd, 100 * bb_sd, k_sd / bb_sd))
  add_row("bb_falsification", "k_park_effect_sd_pct_pts", sprintf("%.4f", 100 * k_sd), "Spread of park K effects")
  add_row("bb_falsification", "bb_park_effect_sd_pct_pts", sprintf("%.4f", 100 * bb_sd), "Should be much smaller than K")
  add_row("bb_falsification", "k_to_bb_sd_ratio", sprintf("%.3f", k_sd / bb_sd), "Higher means K effect is more park-driven than BB")
} else {
  message("\n[2] BB falsification: sidecar output not found.")
}

# ── 3. Era-event falsification ────────────────────────────────────────────────
# Venues with more than one era in the K table: a dimension change should not
# move strikeouts. Compare eras within the same venue.
k$venue_key <- sub("__.*$", "", k$park_era_id)
multi <- names(which(table(k$venue_key) > 1))
if (length(multi) > 0) {
  era_rows <- list()
  for (v in multi) {
    sub <- k[k$venue_key == v, , drop = FALSE]
    sub <- sub[order(-sub$n_pa), ]
    spread <- max(sub$park_effect) - min(sub$park_effect)
    era_rows[[length(era_rows) + 1L]] <- data.frame(
      venue_key = v,
      team = sub$home_team[1],
      n_eras = nrow(sub),
      k_effect_spread_pct_pts = 100 * spread,
      stringsAsFactors = FALSE
    )
  }
  era_df <- do.call(rbind, era_rows)
  era_df <- era_df[order(-era_df$k_effect_spread_pct_pts), ]
  message("\n[3] Era-event falsification: within-venue K spread across eras (K% points)")
  print(utils::head(era_df, 10))
  utils::write.csv(era_df, file.path(output_dir, "k_era_event_check.csv"), row.names = FALSE, na = "")
  add_row("era_falsification", "max_within_venue_k_spread_pct_pts",
          sprintf("%.4f", max(era_df$k_effect_spread_pct_pts)),
          "Dimension changes should not move K; large values need explanation")
  add_row("era_falsification", "median_within_venue_k_spread_pct_pts",
          sprintf("%.4f", stats::median(era_df$k_effect_spread_pct_pts)), "")
} else {
  message("\n[3] Era-event falsification: no venue has multiple eras in the K table.")
}

# ── 4. External agreement with Savant index_so ────────────────────────────────
if (file.exists(savant_csv)) {
  sav <- utils::read.csv(savant_csv, stringsAsFactors = FALSE, check.names = FALSE)
  # Compare on current park configurations only: take each venue's largest era.
  k_cur <- k[order(k$venue_key, -k$n_pa), ]
  k_cur <- k_cur[!duplicated(k_cur$venue_key), , drop = FALSE]
  k_cur$base_park_id <- k_cur$venue_key

  cmp <- merge(
    k_cur[, c("base_park_id", "home_team", "park_effect", "idx_100", "obs_k_rate", "n_pa")],
    sav[, c("base_park_id", "name_display_club", "index_so", "index_bb", "n_pa", "year_range")],
    by = "base_park_id", suffixes = c("_ipf", "_savant")
  )

  if (nrow(cmp) >= 5) {
    # Our park effect on the K% point scale, for a like-for-like read against a
    # ratio index expressed around 100.
    cmp$our_k_pct_pts <- 100 * cmp$park_effect
    cmp$our_ratio_idx <- 100 * (1 + cmp$park_effect / baseline_k)

    r2_effect <- stats::cor(cmp$our_k_pct_pts, cmp$index_so)^2
    message(sprintf("\n[4] Savant agreement (%s): n = %d parks, R^2 = %.4f",
                    unique(cmp$year_range)[1], nrow(cmp), r2_effect))
    message(sprintf("    Spread: ours %.1f to %.1f (ratio idx) vs Savant %.0f to %.0f (index_so)",
                    min(cmp$our_ratio_idx), max(cmp$our_ratio_idx),
                    min(cmp$index_so), max(cmp$index_so)))
    message(sprintf("    SD: ours %.2f idx pts vs Savant %.2f idx pts (Savant does not control for batter/pitcher)",
                    stats::sd(cmp$our_ratio_idx), stats::sd(cmp$index_so)))

    cmp <- cmp[order(-cmp$our_k_pct_pts), ]
    show_cols <- c("home_team", "name_display_club", "our_k_pct_pts", "our_ratio_idx", "idx_100", "index_so", "n_pa_ipf")
    message("\n    Most strikeout-friendly (ours):")
    print(utils::head(cmp[, show_cols], 6))
    message("\n    Least strikeout-friendly (ours):")
    print(utils::tail(cmp[, show_cols], 6))

    utils::write.csv(cmp, file.path(output_dir, "k_savant_comparison.csv"), row.names = FALSE, na = "")
    add_row("savant_agreement", "r_squared_vs_index_so", sprintf("%.4f", r2_effect),
            sprintf("n=%d parks, Savant window %s", nrow(cmp), unique(cmp$year_range)[1]))
    add_row("savant_agreement", "our_ratio_idx_sd", sprintf("%.3f", stats::sd(cmp$our_ratio_idx)),
            "Ours controls for batter/pitcher identity")
    add_row("savant_agreement", "savant_index_so_sd", sprintf("%.3f", stats::sd(cmp$index_so)),
            "Savant is a raw ratio index")

    if (all(c("index_bb", "park_effect") %in% names(cmp))) {
      bbp <- file.path(output_dir, "park_factors_bb_overall.csv")
      if (file.exists(bbp)) {
        bbd <- utils::read.csv(bbp, stringsAsFactors = FALSE, check.names = FALSE)
        bbd$venue_key <- sub("__.*$", "", bbd$park_era_id)
        bbd <- bbd[order(bbd$venue_key, -bbd$n_pa), ]
        bbd <- bbd[!duplicated(bbd$venue_key), , drop = FALSE]
        bbc <- merge(cmp[, c("base_park_id", "index_bb")],
                     data.frame(base_park_id = bbd$venue_key, our_bb = bbd$park_effect, stringsAsFactors = FALSE),
                     by = "base_park_id")
        if (nrow(bbc) >= 5) {
          r2_bb <- stats::cor(bbc$our_bb, bbc$index_bb)^2
          message(sprintf("\n    BB cross-check: R^2 vs Savant index_bb = %.4f (n=%d)", r2_bb, nrow(bbc)))
          add_row("savant_agreement", "r_squared_vs_index_bb", sprintf("%.4f", r2_bb), "")
        }
      }
    }
  } else {
    message("\n[4] Savant agreement: too few matched parks to compare.")
  }
} else {
  message(sprintf("\n[4] Savant agreement: %s not found (run fetch_savant_park_factors.R).", savant_csv))
}

# ── Coors sanity ──────────────────────────────────────────────────────────────
coors <- k[grepl("^venue_19", k$park_era_id), , drop = FALSE]
if (nrow(coors) > 0) {
  message(sprintf("\nCoors sanity: K effect %+.4f K%% points (idx %.1f), n_pa = %s",
                  100 * coors$park_effect[1], coors$idx_100[1], format(coors$n_pa[1], big.mark = ",")))
  add_row("coors_sanity", "k_effect_pct_pts", sprintf("%.4f", 100 * coors$park_effect[1]),
          "Park-adjusted Stuff+ work found Coors suppresses stuff; expect fewer Ks")
}

report <- do.call(rbind, rows)
utils::write.csv(report, report_path, row.names = FALSE, na = "")
message("\nWrote validation report: ", report_path)
