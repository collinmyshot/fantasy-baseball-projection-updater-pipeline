#!/usr/bin/env Rscript
# build_park_factors_by_hand_era.R
# ---------------------------------------------------------------------------
# Derive CLEAN hand-split HR + BACON park factors at the era grain, for the
# hitter-streamonator re-run. The shipped by_hand component files carry a
# broken pf_index_hand scale (validation memory flagged this), so we recover
# the exact resid->idx_100 linear map from the canonical with_id era file and
# apply it to delta_hand (= park_effect + hand_effect, the hand-specific
# residual). Output joins each era's team_id + year range for the backtest.
#
# Output: data/processed/park_factors/park_factors_by_hand_era.csv
#   cols: park_era_id, team_id, team, park, years_used, year_start, year_end,
#         hand (L/R), bacon_idx_hand, hr_idx_hand, n_bbe_bacon, n_bbe_hr
# ---------------------------------------------------------------------------
suppressWarnings(suppressMessages(library(data.table)))
PF <- "/Users/ckaufman/Documents/New project/data/processed/park_factors"

withid <- fread(file.path(PF, "park_factors_savant_style_with_id.csv"))

# recover exact resid -> idx_100 maps (R2 = 1 by construction)
scale_of <- function(comp) {
  r <- withid[[paste0(comp, "_resid")]]; i <- withid[[paste0(comp, "_idx_100")]]
  ok <- is.finite(r) & is.finite(i) & r != 0
  b <- coef(lm(i[ok] ~ r[ok]))          # intercept ~100, slope = scale
  stopifnot(abs(b[1] - 100) < 1e-6)
  as.numeric(b[2])
}
SC <- c(bacon = scale_of("bacon"), hr = scale_of("hr"))
cat(sprintf("recovered scales: bacon=%.4f  hr=%.4f\n", SC["bacon"], SC["hr"]))

# NOTE: with_id "team_id" is actually the abbreviation; the numeric MLBAM id
# (what the spine joins on) comes from mlbam_team_map via the full-name "team".
tmap <- fread(file.path("/Users/ckaufman/Documents/New project/data/raw/mlbam_team_map.csv"))
era_map <- withid[, .(park_era_id, team_abbr = team_id, team, park, years_used)]
era_map <- merge(era_map, tmap, by.x = "team", by.y = "club", all.x = TRUE)  # -> numeric team_id
if (anyNA(era_map$team_id)) {
  cat("WARNING: unmapped teams:\n"); print(unique(era_map[is.na(team_id), .(team, team_abbr)]))
}
era_map[, `:=`(year_start = as.integer(sub("-.*", "", years_used)),
               year_end   = as.integer(sub(".*-", "", years_used)))]

read_hand <- function(file, comp) {
  d <- fread(file.path(PF, file))
  d[, .(park_era_id, hand, n_bbe, idx = 100 + SC[[comp]] * delta_hand)]
}
bacon <- read_hand("park_factors_bacon_by_hand.csv", "bacon")
hr    <- read_hand("park_factors_hr_by_hand.csv",    "hr")

out <- merge(bacon[, .(park_era_id, hand, bacon_idx_hand = idx, n_bbe_bacon = n_bbe)],
             hr[,    .(park_era_id, hand, hr_idx_hand    = idx, n_bbe_hr    = n_bbe)],
             by = c("park_era_id", "hand"))
out <- merge(era_map, out, by = "park_era_id")
setorder(out, team, -year_start, hand)
fwrite(out, file.path(PF, "park_factors_by_hand_era.csv"))
cat("wrote", nrow(out), "rows (era x hand) to park_factors_by_hand_era.csv\n\n")

# ------------------------- SANITY CHECKS (ground truth) --------------------
cur <- out[year_end >= 2026]                      # current-park eras only
wide <- dcast(cur, team + park + park_era_id ~ hand,
              value.var = c("bacon_idx_hand", "hr_idx_hand"))

cat("=== HR park factor by hand, current eras (sorted by L-R gap) ===\n")
wide[, hr_LmR := hr_idx_hand_L - hr_idx_hand_R]
print(wide[order(-hr_LmR), .(team, park,
      HR_L = round(hr_idx_hand_L, 1), HR_R = round(hr_idx_hand_R, 1),
      `HR_L-R` = round(hr_LmR, 1))], nrow = 40)

cat("\n=== named ground-truth checks ===\n")
gt <- function(tm, lbl) {
  r <- wide[team == tm]
  if (!nrow(r)) { cat(sprintf("  %-4s (%s): NOT FOUND\n", tm, lbl)); return(invisible()) }
  cat(sprintf("  %-4s %-22s HR L/R = %.1f / %.1f (L-R %+.1f) | BACON L/R = %.1f / %.1f\n",
      tm, lbl, r$hr_idx_hand_L, r$hr_idx_hand_R, r$hr_idx_hand_L - r$hr_idx_hand_R,
      r$bacon_idx_hand_L, r$bacon_idx_hand_R))
}
gt("Rockies", "Coors: symmetric, high")
gt("Yankees", "Yankee St: LHB HR>>")
gt("Red Sox", "Fenway: HR L<R (memory 86/92)")
gt("Reds",    "GABP: LHB lean")

cat(sprintf("\n=== L-R HR gap spread (current parks): sd=%.1f, range [%.1f, %.1f] ===\n",
    sd(wide$hr_LmR), min(wide$hr_LmR), max(wide$hr_LmR)))
cat(sprintf("=== L-R BACON gap spread: sd=%.2f (expect << HR, per PF memo) ===\n",
    sd(wide$bacon_idx_hand_L - wide$bacon_idx_hand_R)))
