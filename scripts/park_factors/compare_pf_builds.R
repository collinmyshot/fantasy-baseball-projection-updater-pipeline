#!/usr/bin/env Rscript
# Compare two park factor builds (e.g. pre- vs post-drag-correction).
#
# Two views:
#   1. Model level — delta_woba_over_xwoba_overall per park_era_id from
#      park_factors_overall.csv, plus HR component deltas. Same units in both
#      builds, unaffected by display standardization: this is the honest
#      "what did the model change its mind about" comparison.
#   2. Display level — the clean 2026 tables joined by Team. Index values are
#      NOT directly comparable across builds (the new build re-standardizes
#      over the 30 current parks), so ranks are the meaningful column here.

source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  old_dir = list(flag = "--old-dir", default = file.path("data", "processed", "park_factors", "_pre_drag_snapshot_20260702")),
  new_dir = list(flag = "--new-dir", default = file.path("data", "processed", "park_factors")),
  out_csv = list(flag = "--out",     default = file.path("data", "processed", "park_factors", "build_comparison_20260702.csv"))
))

old_dir <- parsed$old_dir
new_dir <- parsed$new_dir
out_csv <- parsed$out_csv

read_or_stop <- function(dir, file) {
  path <- file.path(dir, file)
  if (!file.exists(path)) {
    stop(sprintf("Missing: %s", path))
  }
  utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
}

# ── View 1: model-level era deltas ───────────────────────────────────────────
old_overall <- read_or_stop(old_dir, "park_factors_overall.csv")
new_overall <- read_or_stop(new_dir, "park_factors_overall.csv")
old_hr <- read_or_stop(old_dir, "park_factors_hr_overall.csv")
new_hr <- read_or_stop(new_dir, "park_factors_hr_overall.csv")

sel <- function(d, delta_col, tag) {
  out <- d[, c("park_era_id", "home_team", delta_col)]
  names(out)[3] <- tag
  out
}

cmp <- merge(
  sel(old_overall, "delta_woba_over_xwoba_overall", "woba_delta_old"),
  sel(new_overall, "delta_woba_over_xwoba_overall", "woba_delta_new")[, c(1, 3)],
  by = "park_era_id"
)
cmp <- merge(cmp, sel(old_hr, "delta_overall", "hr_delta_old")[, c(1, 3)], by = "park_era_id", all.x = TRUE)
cmp <- merge(cmp, sel(new_hr, "delta_overall", "hr_delta_new")[, c(1, 3)], by = "park_era_id", all.x = TRUE)
cmp <- merge(cmp, new_overall[, c("park_era_id", "n_bbe")], by = "park_era_id", all.x = TRUE)

cmp$woba_delta_change <- cmp$woba_delta_new - cmp$woba_delta_old
cmp$hr_delta_change <- cmp$hr_delta_new - cmp$hr_delta_old
cmp <- cmp[order(-abs(cmp$woba_delta_change)), ]
rownames(cmp) <- NULL

utils::write.csv(cmp, out_csv, row.names = FALSE, na = "")

message("=== Model-level: biggest wOBA-delta movers (new - old) ===")
top <- head(cmp, 12)
for (i in seq_len(nrow(top))) {
  message(sprintf(
    "  %-38s %-4s  woba: %+0.4f -> %+0.4f (%+0.4f)   hr: %+0.5f -> %+0.5f",
    top$park_era_id[i], top$home_team[i],
    top$woba_delta_old[i], top$woba_delta_new[i], top$woba_delta_change[i],
    ifelse(is.na(top$hr_delta_old[i]), NA, top$hr_delta_old[i]),
    ifelse(is.na(top$hr_delta_new[i]), NA, top$hr_delta_new[i])
  ))
}
message(sprintf("Correlation old vs new wOBA deltas: %.4f (n=%s eras)",
                stats::cor(cmp$woba_delta_old, cmp$woba_delta_new, use = "complete.obs"), nrow(cmp)))

# ── View 2: display-level clean tables ───────────────────────────────────────
old_clean <- read_or_stop(old_dir, "park_factors_savant_style_clean_2026.csv")
new_clean <- read_or_stop(new_dir, "park_factors_savant_style_clean_2026.csv")

j <- merge(
  old_clean[, c("Team", "Park", "Rank", "Overall Park Factor", "HR Park Factor")],
  new_clean[, intersect(c("Team", "Park", "Rank", "Overall Park Factor", "HR Park Factor", "Carry Park Factor", "Carry (ft vs avg)"), names(new_clean))],
  by = "Team",
  suffixes = c("_old", "_new")
)
j$rank_change <- j$Rank_old - j$Rank_new
j <- j[order(-abs(j$rank_change)), ]

message("")
message("=== Display-level: biggest rank movers (positive = climbed) ===")
topj <- head(j[j$rank_change != 0, ], 10)
for (i in seq_len(nrow(topj))) {
  message(sprintf(
    "  %-12s rank %2d -> %2d (%+d)   overall %6.2f -> %6.2f",
    topj$Team[i], topj$Rank_old[i], topj$Rank_new[i], topj$rank_change[i],
    topj$`Overall Park Factor_old`[i], topj$`Overall Park Factor_new`[i]
  ))
}
message(sprintf("Rank correlation (Spearman): %.4f",
                stats::cor(j$Rank_old, j$Rank_new, method = "spearman")))
message("Full comparison written to: ", out_csv)
