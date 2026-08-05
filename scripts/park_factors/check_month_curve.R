#!/usr/bin/env Rscript
# Sanity check for the August 2026 month-bucket seasonal fixed effect.
#
# The build replaced the league-wide `half` fixed effect with a 6-level month
# bucket. Two questions:
#   1. Does the fitted month curve rise into the summer and fall back in
#      September/October, the shape Tango's temperature story predicts (warmer
#      air is less dense, so the ball carries and offense rises)?
#   2. Does the fitted curve agree with the raw seasonal pattern in the data
#      (monthly HR per BBE), rather than contradicting it?
#
# This does NOT re-fit anything. It reads the build's own fixed-effect output
# and the BBE store, so it is cheap and can run any time after a build.
#
# The old half fixed effect is printed alongside for continuity: the month
# curve should straddle it, since it is the same seasonal signal at finer
# resolution.

source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  output_dir   = list(flag = "--output-dir",   default = file.path("data", "processed", "park_factors")),
  baseline_dir = list(flag = "--baseline-dir", default = ""),
  bbe_input    = list(flag = "--bbe-input",    default = file.path("data", "raw", "statcast_bbe_store.csv")),
  max_date     = list(flag = "--max-date",     default = ""),
  out_csv      = list(flag = "--out",          default = file.path("data", "processed", "park_factors", "month_curve_check.csv"))
))

output_dir   <- parsed$output_dir
baseline_dir <- parsed$baseline_dir
bbe_input    <- parsed$bbe_input
max_date     <- suppressWarnings(as.Date(as.character(parsed$max_date)))
out_csv      <- parsed$out_csv

fx_path <- file.path(output_dir, "park_factor_fixed_effects.csv")
if (!file.exists(fx_path)) {
  stop(sprintf("Missing fixed effects file: %s", fx_path))
}
fx <- utils::read.csv(fx_path, stringsAsFactors = FALSE, check.names = FALSE)

main <- fx[fx$model == "woba_over_xwoba", , drop = FALSE]
month_terms <- main[grepl("^month_grp", main$term), , drop = FALSE]
if (nrow(month_terms) == 0) {
  stop("No month_grp fixed effects found. Did this build use the month bucket?")
}

# mar_apr is the reference level (coefficient 0 by construction).
curve <- data.frame(
  bucket = c("mar_apr", sub("^month_grp", "", month_terms$term)),
  effect_woba = c(0, month_terms$estimate),
  stringsAsFactors = FALSE
)
lvls <- c("mar_apr", "may", "jun", "jul", "aug", "sep_oct")
curve <- curve[match(lvls, curve$bucket), , drop = FALSE]
curve <- curve[!is.na(curve$bucket), , drop = FALSE]
curve$effect_x1000 <- 1000 * curve$effect_woba

message("=== Fitted month curve (main wOBAcon model, vs mar_apr, x1000) ===")
for (i in seq_len(nrow(curve))) {
  message(sprintf("  %-8s %+7.2f", curve$bucket[i], curve$effect_x1000[i]))
}

peak <- curve$bucket[which.max(curve$effect_woba)]
sep_below_peak <- curve$effect_woba[curve$bucket == "sep_oct"] < max(curve$effect_woba)
message(sprintf("\nPeak bucket: %s | Sep/Oct below peak: %s", peak, sep_below_peak))
message(sprintf("Shape consistent with the warm-air story: %s",
                if (peak %in% c("jul", "aug") && isTRUE(sep_below_peak)) "YES" else "REVIEW"))

# Continuity with the retired half fixed effect.
if (nzchar(baseline_dir)) {
  bpath <- file.path(baseline_dir, "park_factor_fixed_effects.csv")
  if (file.exists(bpath)) {
    bfx <- utils::read.csv(bpath, stringsAsFactors = FALSE, check.names = FALSE)
    h <- bfx$estimate[bfx$model == "woba_over_xwoba" & bfx$term == "half2H"]
    if (length(h) == 1) {
      # 1H = mar_apr/may/jun, 2H = jul/aug/sep_oct, weighted below by real PA
      # counts; here just the unweighted mean for a quick read.
      m1 <- mean(curve$effect_woba[curve$bucket %in% c("mar_apr", "may", "jun")])
      m2 <- mean(curve$effect_woba[curve$bucket %in% c("jul", "aug", "sep_oct")])
      message(sprintf(
        "\nPrior build half2H = %+.5f | this build implied 2H-1H gap = %+.5f (unweighted bucket means)",
        h, m2 - m1
      ))
    }
  }
}

# ── Raw seasonal pattern in the data, as an independent check ────────────────
if (file.exists(bbe_input)) {
  message("\nReading BBE store for the raw monthly pattern (this takes a moment)...")
  bbe <- utils::read.csv(bbe_input, stringsAsFactors = FALSE, check.names = FALSE)
  bbe$game_date <- as.Date(bbe$game_date)
  bbe <- bbe[!is.na(bbe$game_date), ]
  if (!is.na(max_date)) {
    bbe <- bbe[bbe$game_date <= max_date, ]
  }
  bbe$season <- as.integer(format(bbe$game_date, "%Y"))
  bbe <- bbe[bbe$season != 2020, ]
  bbe$month <- as.integer(format(bbe$game_date, "%m"))
  bucket_map <- c("3" = "mar_apr", "4" = "mar_apr", "5" = "may", "6" = "jun",
                  "7" = "jul", "8" = "aug", "9" = "sep_oct", "10" = "sep_oct")
  bbe$bucket <- bucket_map[as.character(bbe$month)]
  bbe <- bbe[!is.na(bbe$bucket), ]
  bbe$is_hr <- as.integer(tolower(trimws(bbe$events)) == "home_run")

  raw <- stats::aggregate(list(hr_per_bbe = bbe$is_hr), by = list(bucket = bbe$bucket), FUN = mean)
  n_by <- stats::aggregate(list(n_bbe = rep(1L, nrow(bbe))), by = list(bucket = bbe$bucket), FUN = sum)
  raw <- merge(raw, n_by, by = "bucket")
  raw <- raw[match(lvls, raw$bucket), , drop = FALSE]
  raw <- raw[!is.na(raw$bucket), , drop = FALSE]
  base_hr <- raw$hr_per_bbe[raw$bucket == "mar_apr"]
  raw$pct_vs_mar_apr <- 100 * (raw$hr_per_bbe / base_hr - 1)

  message("\n=== Raw HR per BBE by month bucket ===")
  for (i in seq_len(nrow(raw))) {
    message(sprintf("  %-8s %.4f  (%+.1f%% vs mar_apr, n=%s)",
                    raw$bucket[i], raw$hr_per_bbe[i], raw$pct_vs_mar_apr[i],
                    format(raw$n_bbe[i], big.mark = ",")))
  }

  curve <- merge(curve, raw[, c("bucket", "hr_per_bbe", "pct_vs_mar_apr", "n_bbe")], by = "bucket", all.x = TRUE)
  curve <- curve[match(lvls, curve$bucket), , drop = FALSE]
  curve <- curve[!is.na(curve$bucket), , drop = FALSE]

  if (sum(is.finite(curve$effect_woba) & is.finite(curve$pct_vs_mar_apr)) >= 3) {
    r2 <- stats::cor(curve$effect_woba, curve$pct_vs_mar_apr, use = "complete.obs")^2
    message(sprintf("\nFitted month effect vs raw HR pattern: R^2 = %.4f across %d buckets",
                    r2, nrow(curve)))
  }
} else {
  message(sprintf("\nBBE store not found at %s; skipping the raw monthly comparison.", bbe_input))
}

utils::write.csv(curve, out_csv, row.names = FALSE, na = "")
message("\nWrote: ", out_csv)
