#!/usr/bin/env Rscript
# Era archive + build-history ledger.
#
# Every build re-estimates every park era (a closed era's data is frozen, so
# its estimate barely moves; only shared terms drift). This script exposes all
# of them in one browsable table and appends each build's published numbers to
# a permanent ledger, so any past published value stays retrievable even as
# the model evolves.
#
# Index scale: all eras are expressed on the CURRENT 30-park scale (mean/SD of
# the picked 2026 rows), so "Walltimore 96" means 96 relative to today's
# average park, directly comparable with the live table.
#
# Outputs:
#   park_factor_era_archive.csv   full archive (current + historical eras)
#   pf_build_history.csv          append-only ledger keyed by build_date

source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  output_dir   = list(flag = "--output-dir",   default = file.path("data", "processed", "park_factors")),
  min_bbe      = list(flag = "--min-bbe",      default = 800, type = "numeric"),
  data_through = list(flag = "--data-through", default = "")
))

output_dir <- parsed$output_dir
min_bbe <- as.numeric(parsed$min_bbe)
data_through <- as.character(parsed$data_through)

with_id <- utils::read.csv(file.path(output_dir, "park_factors_savant_style_with_id.csv"), stringsAsFactors = FALSE, check.names = FALSE)
picked <- utils::read.csv(file.path(output_dir, "park_factors_savant_style_clean_2026_with_id.csv"), stringsAsFactors = FALSE, check.names = FALSE)
overall_meta <- utils::read.csv(file.path(output_dir, "park_factors_overall.csv"), stringsAsFactors = FALSE, check.names = FALSE)

hand_path <- file.path(output_dir, "park_factors_by_hand.csv")
hand <- if (file.exists(hand_path)) {
  utils::read.csv(hand_path, stringsAsFactors = FALSE, check.names = FALSE)
} else {
  data.frame()
}

if (!nzchar(data_through)) {
  meta_path <- file.path(output_dir, "run_metadata.csv")
  if (file.exists(meta_path)) {
    rm_tbl <- utils::read.csv(meta_path, stringsAsFactors = FALSE)
    hit <- rm_tbl$value[rm_tbl$key %in% c("max_date", "data_through")]
    if (length(hit) > 0 && nzchar(hit[[1]])) {
      data_through <- hit[[1]]
    }
  }
}

arch <- with_id
arch$total_bbe <- suppressWarnings(as.numeric(arch$total_bbe))
arch <- arch[is.finite(arch$total_bbe) & arch$total_bbe >= min_bbe, ]

arch$status <- ifelse(arch$park_era_id %in% picked$park_era_id, "current", "historical")

# Historical base-configuration eras get their true venue name from the
# schedule metadata (Turner Field, Oakland Coliseum, Globe Life Park...)
# instead of the team's current default park name.
vn <- overall_meta[, c("park_era_id", "venue_name")]
arch <- merge(arch, vn, by = "park_era_id", all.x = TRUE)
arch$suffix <- sub("^.*__", "", arch$park_era_id)
use_vn <- arch$status == "historical" & arch$suffix == "base" &
  !is.na(arch$venue_name) & nzchar(trimws(arch$venue_name)) &
  tolower(trimws(arch$venue_name)) != "unknown_venue"
arch$park[use_vn] <- arch$venue_name[use_vn]

era_label <- function(suffix) {
  s <- gsub("_", " ", suffix)
  ifelse(suffix == "base", "base configuration", s)
}
arch$era <- era_label(arch$suffix)

# Current-30 scale for every column: mean/SD from the picked 2026 rows.
scale_from_picked <- function(col) {
  v <- suppressWarnings(as.numeric(picked[[col]]))
  v <- v[is.finite(v)]
  list(m = mean(v), s = stats::sd(v))
}
idx_on <- function(x, sc) {
  x <- suppressWarnings(as.numeric(x))
  ifelse(is.finite(x), 100 + 10 * (x - sc$m) / sc$s, NA_real_)
}

sc_overall <- scale_from_picked("overall_resid")
sc_bacon <- scale_from_picked("bacon_resid")
sc_hr <- scale_from_picked("hr_resid")
sc_carry <- scale_from_picked("carry_ft")

arch$overall_idx <- idx_on(arch$overall_resid, sc_overall)
arch$bacon_idx <- idx_on(arch$bacon_resid, sc_bacon)
arch$hr_idx <- idx_on(arch$hr_resid, sc_hr)
arch$carry_idx <- idx_on(arch$carry_ft, sc_carry)

# Batter-side overall factors, standardized per side over the current 30
# parks — the same convention the LHB/RHB leaderboard views use, so the same
# era shows the same number in both places. Each column reads "vs the average
# current park for that batter side."
if (nrow(hand) > 0) {
  for (side in c("L", "R")) {
    h <- hand[hand$hand == side, c("park_era_id", "delta_hand")]
    names(h)[2] <- "delta_side"
    arch <- merge(arch, h, by = "park_era_id", all.x = TRUE)
    side_current <- h$delta_side[h$park_era_id %in% picked$park_era_id]
    side_current <- side_current[is.finite(side_current)]
    sc_side <- list(m = mean(side_current), s = stats::sd(side_current))
    arch[[paste0(tolower(side), "hb_overall_idx")]] <- idx_on(arch$delta_side, sc_side)
    arch$delta_side <- NULL
  }
} else {
  arch$lhb_overall_idx <- NA_real_
  arch$rhb_overall_idx <- NA_real_
}

out_cols <- c(
  "park_era_id", "status", "team_id", "team", "park", "era", "years_used",
  "total_bbe", "overall_idx", "bacon_idx", "hr_idx", "carry_idx", "carry_ft",
  "lhb_overall_idx", "rhb_overall_idx", "overall_resid", "overall_se"
)
out_cols <- out_cols[out_cols %in% names(arch)]
arch_out <- arch[, out_cols]

cur <- arch_out[arch_out$status == "current", ]
cur <- cur[order(-cur$overall_idx), ]
hist <- arch_out[arch_out$status == "historical", ]
hist <- hist[order(hist$team, hist$years_used), ]
arch_out <- rbind(cur, hist)
rownames(arch_out) <- NULL

for (nm in c("overall_idx", "bacon_idx", "hr_idx", "carry_idx", "carry_ft", "lhb_overall_idx", "rhb_overall_idx")) {
  if (nm %in% names(arch_out)) {
    arch_out[[nm]] <- round(as.numeric(arch_out[[nm]]), 2)
  }
}

utils::write.csv(arch_out, file.path(output_dir, "park_factor_era_archive.csv"), row.names = FALSE, na = "")
message(sprintf(
  "Wrote era archive: %s current + %s historical eras (min %s BBE).",
  sum(arch_out$status == "current"), sum(arch_out$status == "historical"), min_bbe
))

# Append-only ledger of published numbers per build.
ledger_path <- file.path(output_dir, "pf_build_history.csv")
ledger_new <- cbind(
  build_date = format(Sys.Date(), "%Y-%m-%d"),
  data_through = data_through,
  arch_out
)
if (file.exists(ledger_path)) {
  ledger <- utils::read.csv(ledger_path, stringsAsFactors = FALSE, check.names = FALSE)
  ledger <- ledger[!(ledger$build_date == ledger_new$build_date[1]), ]
  common <- intersect(names(ledger), names(ledger_new))
  ledger <- rbind(ledger[, common], ledger_new[, common])
} else {
  ledger <- ledger_new
}
utils::write.csv(ledger, ledger_path, row.names = FALSE, na = "")
message(sprintf(
  "Ledger updated: %s (%s builds, %s rows).",
  ledger_path, length(unique(ledger$build_date)), nrow(ledger)
))
