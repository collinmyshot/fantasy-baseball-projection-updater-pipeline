#!/usr/bin/env Rscript
# Pilot: is a month-bucket fixed effect worth a full 2h rebuild?
#
# Test A (full data, instant): half is nested inside the month buckets
# (mar_apr/may/jun subdivide 1H; jul/aug/sep_oct subdivide 2H), so a nested
# F-test on the league-level residual answers "is there month structure the
# half term misses?" Run on both the wOBAcon residual and the HR residual.
#
# Test B (250K-row sample): fit the full mixed model twice (half vs month
# buckets) and compare the park-era effects. If they correlate ~1, the rebuild
# is about hygiene and cleaner park-half splits, not about rankings.

source(file.path("R", "utils.R"))
source(file.path("R", "park_factors.R"))
suppressPackageStartupMessages(library(lme4))

message("Loading store + supporting data...")
raw <- utils::read.csv("data/raw/statcast_bbe_store.csv", stringsAsFactors = FALSE, check.names = FALSE)
sched <- utils::read.csv("data/raw/mlb_schedule_venues.csv", stringsAsFactors = FALSE, check.names = FALSE)
events <- load_park_events("data/manual/park_era_events.csv")
defense <- build_defense_composite(utils::read.csv("data/manual/team_defense_2015_2025.csv", stringsAsFactors = FALSE, check.names = FALSE))
drag <- utils::read.csv("data/processed/drag_daily.csv", stringsAsFactors = FALSE, check.names = FALSE)

md <- prepare_bbe_model_data(
  bbe_raw = raw, schedule_data = sched, park_events = events,
  defense_data = defense, drag_data = drag
)
md <- md[md$game_date <= as.Date("2026-06-30"), ]
message(sprintf("Modeling rows: %s", nrow(md)))

bucket <- c("3" = "mar_apr", "4" = "mar_apr", "5" = "may", "6" = "jun",
            "7" = "jul", "8" = "aug", "9" = "sep_oct", "10" = "sep_oct")
md$month_grp <- factor(bucket[as.character(md$month)],
                       levels = c("mar_apr", "may", "jun", "jul", "aug", "sep_oct"))

message("\n=== Test A: league-level month structure beyond half (full data) ===")
for (outcome in c("resid", "hr_resid")) {
  f_half <- stats::lm(stats::as.formula(paste(outcome, "~ half")), data = md)
  f_month <- stats::lm(stats::as.formula(paste(outcome, "~ month_grp")), data = md)
  a <- stats::anova(f_half, f_month)
  co <- stats::coef(f_month)
  message(sprintf("%s: F = %.1f, p = %.3g", outcome, a$F[2], a$`Pr(>F)`[2]))
  message(sprintf("  month curve (vs mar_apr, x1000): may %+.2f  jun %+.2f  jul %+.2f  aug %+.2f  sep_oct %+.2f",
                  1000 * co[["month_grpmay"]], 1000 * co[["month_grpjun"]], 1000 * co[["month_grpjul"]],
                  1000 * co[["month_grpaug"]], 1000 * co[["month_grpsep_oct"]]))
}

message("\n=== Test B: park-effect stability under sampled dual fit (n = 250K) ===")
set.seed(42)
samp <- md[sort(sample.int(nrow(md), 250000)), ]

rhs_re <- "(1 | park_era_id) + (1 | park_era_half_id) + (1 | batter_season_id) + (1 | pitcher_season_id) + (1 | fielding_team_season_id) + (1 | batting_team_season_id)"
fixed_common <- "drag_c + drag_missing + defense_c + defense_missing"

fit_variant <- function(season_term) {
  form <- stats::as.formula(sprintf("resid ~ %s + %s + %s", season_term, fixed_common, rhs_re))
  lme4::lmer(form, data = samp, weights = samp$quality_weight, REML = TRUE,
             control = lme4::lmerControl(optimizer = "bobyqa", calc.derivs = FALSE))
}

message("Fitting half variant...")
m_half <- fit_variant("half")
message("Fitting month variant...")
m_month <- fit_variant("month_grp")

re_h <- lme4::ranef(m_half)$park_era_id
re_m <- lme4::ranef(m_month)$park_era_id
common <- intersect(rownames(re_h), rownames(re_m))
v_h <- re_h[common, 1]
v_m <- re_m[common, 1]
message(sprintf("Park-era effects: correlation = %.5f across %s eras", stats::cor(v_h, v_m), length(common)))
d <- abs(v_h - v_m)
ord <- order(-d)[1:5]
for (i in ord) {
  message(sprintf("  biggest shift: %-38s %+.5f -> %+.5f (delta %.5f)", common[i], v_h[i], v_m[i], d[i]))
}

fe <- lme4::fixef(m_month)
mg <- fe[grep("^month_grp", names(fe))]
message("Month fixed effects in the mixed model (vs mar_apr, x1000): ",
        paste(sprintf("%s %+.2f", sub("month_grp", "", names(mg)), 1000 * mg), collapse = "  "))

message("\nPilot complete.")
