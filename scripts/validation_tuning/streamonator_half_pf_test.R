#!/usr/bin/env Rscript
# streamonator_half_pf_test.R
#
# Edge-case test (August 2026): does giving each start the 1H / 2H park factor
# instead of the season-long one improve Streamonator scoring?
#
# The Streamonator scores a start as a 6:3:1 blend of SP Skillz, Team Rater and
# the inverted Park Factor. Today the PF slot gets one number per park-era. The
# iPF build also emits a 1H / 2H split (park x half random effect on top of the
# park effect), so a start in April could be scored on the April-June version of
# its park and a start in August on the July-October version.
#
# DELIBERATELY GENEROUS SETUP. The half values are fit on all seasons including
# the season each start comes from, so the half adjustment gets to peek at the
# very starts it is being graded on. That is an upper bound, not something a
# forward-looking tool could reproduce. If the generous version does nothing,
# the honest version cannot do better.
#
# Scoreboard is the pre-registered one from streamonator_lens_ladder.R so this
# rung is directly comparable to S-1 / S0 / S1 / S2 / S3:
#   M1q  ER5+ rate inside the top start-zone share        (primary, lower better)
#   M2q  share of all ER5+ starts landing in the sit zone (higher better)
#   M3q  good-start rate inside the start zone            (higher better)
#   rho  Spearman vs GSM 0-4                              (continuity metric)
# Adoption needs all three: >= 2% relative M1q gain, bootstrap 95% CI excluding
# zero, and the direction holding in >= 4 of 5 seasons.
#
# Half rule matches the model: months 3-6 = 1H, months 7-10 = 2H
# (R/park_factors.R:570).
#
# Usage: Rscript scripts/validation_tuning/streamonator_half_pf_test.R
# Fully cached, no API calls.

suppressWarnings(suppressMessages({
  library(stats)
}))

# Park factors come from the local (August) build; the starts cache is
# gitignored and lives only in the main project root.
PF_DIR <- file.path("data", "processed", "park_factors")
starts_rel <- file.path("data", "processed", "streamonator_weight_analysis")
STARTS_DIR <- if (dir.exists(starts_rel)) starts_rel else file.path("..", "..", "..", starts_rel)
if (!dir.exists(STARTS_DIR)) stop("Cannot locate streamonator_weight_analysis cache")

IN      <- file.path(STARTS_DIR, "starts_with_pf_lenses.csv")
PF_ERA  <- file.path(PF_DIR, "park_factors_savant_style_with_id.csv")
PF_HALF <- file.path(PF_DIR, "park_factors_by_half.csv")
PF_BACON_HALF <- file.path(PF_DIR, "park_factors_bacon_by_half.csv")
OUT     <- file.path(STARTS_DIR, "streamonator_half_pf_results.csv")

SEASONS <- 2021:2025
set.seed(42)

# ── Load ──────────────────────────────────────────────────────────────────────
st  <- read.csv(IN, stringsAsFactors = FALSE)
pf  <- read.csv(PF_ERA, stringsAsFactors = FALSE)
bh  <- read.csv(PF_HALF, stringsAsFactors = FALSE)
bhb <- read.csv(PF_BACON_HALF, stringsAsFactors = FALSE)

# ── Rescale annual and half onto ONE common index scale ──────────────────────
# The published index is 100 + 10 * z, standardized WITHIN each table. Running
# std_index() separately on the annual and half tables would give them different
# means and SDs, which alone would move the composite. Both must be put on the
# annual table's scale so the only thing that changes is the park-half deviation.
common_scale <- function(resid_ref, x) {
  mu <- mean(resid_ref, na.rm = TRUE)
  s  <- sd(resid_ref, na.rm = TRUE)
  100 + 10 * (x - mu) / s
}
# Sanity: the identity the rescale depends on.
stopifnot(max(abs(pf$overall_resid -
                  bh$delta_woba_over_xwoba_overall[match(pf$park_era_id, bh$park_era_id)]),
              na.rm = TRUE) < 1e-12)

bh$idx_ann  <- common_scale(pf$overall_resid, bh$delta_woba_over_xwoba_overall)
bh$idx_half <- common_scale(pf$overall_resid, bh$delta_woba_over_xwoba_half)
bhb$idx_ann  <- common_scale(pf$bacon_resid, bhb$delta_overall)
bhb$idx_half <- common_scale(pf$bacon_resid, bhb$delta_half)

cat(sprintf("Park-half deviations (index pts): n=%d  SD=%.2f  mean|d|=%.2f  max|d|=%.2f\n",
            nrow(bh), sd(bh$idx_half - bh$idx_ann, na.rm = TRUE),
            mean(abs(bh$idx_half - bh$idx_ann), na.rm = TRUE),
            max(abs(bh$idx_half - bh$idx_ann), na.rm = TRUE)))

# ── Attach annual + half PF to each start ────────────────────────────────────
st$month <- as.integer(format(as.Date(st$game_date), "%m"))
st$half   <- ifelse(st$month >= 3 & st$month <= 6, "1H",
             ifelse(st$month >= 7 & st$month <= 10, "2H", NA_character_))

pull <- function(tbl, key_col, keys, val) tbl[[val]][match(keys, tbl[[key_col]])]

st$pf_ann_overall  <- pull(bh,  "park_era_id", st$park_era_id, "idx_ann")
st$pf_ann_bacon    <- pull(bhb, "park_era_id", st$park_era_id, "idx_ann")
hkey <- paste(st$park_era_id, st$half)
st$pf_half_overall <- bh$idx_half[match(hkey, paste(bh$park_era_id, bh$half))]
st$pf_half_bacon   <- bhb$idx_half[match(hkey, paste(bhb$park_era_id, bhb$half))]

# 12 park-eras only have one half of data (neutral sites, temporary parks).
# Those starts fall back to the annual value so the two arms cover the same rows.
n_fallback <- sum(is.na(st$pf_half_overall) & !is.na(st$pf_ann_overall))
st$pf_half_overall[is.na(st$pf_half_overall)] <- st$pf_ann_overall[is.na(st$pf_half_overall)]
st$pf_half_bacon[is.na(st$pf_half_bacon)]     <- st$pf_ann_bacon[is.na(st$pf_half_bacon)]

st <- st[!st$spz_placeholder & !is.na(st$pf_ann_overall) & !is.na(st$half), ]
cat(sprintf("Sample: %d starts | 1H %d / 2H %d | annual fallback on %d starts\n",
            nrow(st), sum(st$half == "1H"), sum(st$half == "2H"), n_fallback))

# ── Composite + scoreboard (identical to streamonator_lens_ladder.R) ─────────
inv <- function(x) 200 - x
composite <- function(pf_idx, w = c(6, 3, 1)) {
  vals <- cbind(st$sp_skillz_index, st$team_rater_inv, inv(pf_idx))
  wm <- matrix(w, nrow(vals), 3, byrow = TRUE); ok <- !is.na(vals)
  rowSums(vals * wm * ok, na.rm = TRUE) / rowSums(wm * ok)
}

sc_ann   <- composite(st$pf_ann_overall)
sc_half  <- composite(st$pf_half_overall)
sc_annB  <- composite(st$pf_ann_bacon)
sc_halfB <- composite(st$pf_half_bacon)

TOP_SHARE <- mean(sc_ann > 105)
BOT_SHARE <- mean(sc_ann < 95)
cat(sprintf("Fixed zone shares from the annual arm: start %.1f%% | sit %.1f%%\n\n",
            100 * TOP_SHARE, 100 * BOT_SHARE))

scoreboard <- function(score, idx = rep(TRUE, nrow(st))) {
  s <- score[idx]; blow <- st$blowup_er5[idx]; good <- st$good_start[idx]; gsm <- st$gsm[idx]
  hi <- s >= quantile(s, 1 - TOP_SHARE); lo <- s <= quantile(s, BOT_SHARE)
  c(M1q = mean(blow[hi]),
    M2q = sum(blow & lo) / sum(blow),
    M3q = mean(good[hi]),
    rho = suppressWarnings(cor(s, gsm, method = "spearman")))
}

# ── How much does the score actually move? ───────────────────────────────────
d_sc <- sc_half - sc_ann
cat("--- Score movement (Overall lens) ---\n")
cat(sprintf("delta composite: SD %.4f | mean|d| %.4f | max|d| %.4f pts\n",
            sd(d_sc), mean(abs(d_sc)), max(abs(d_sc))))

# Decision boundaries as the LIVE app draws them. The shipped Streamonator
# split the 95-105 coin flip at 100 into Lean Bench / Lean Start on 2026-08-14
# (fbb-tools 598c64b), so 100 is a real boundary now and it sits in the
# densest part of the score distribution. Reporting only 105/95 understates
# how many decisions a scoring change can move.
for (b in c(95, 100, 105)) {
  n_cross <- sum((sc_ann > b) != (sc_half > b))
  cat(sprintf("starts crossing the %3d line: %4d of %d (%.3f%%)%s\n",
              b, n_cross, nrow(st), 100 * n_cross / nrow(st),
              if (b == 100) "   <- Lean Bench / Lean Start" else ""))
}
cut4 <- function(s) cut(s, c(-Inf, 95, 100, 105, Inf),
                        labels = c("Sit", "Lean Bench", "Lean Start", "Start"))
b_ann <- cut4(sc_ann); b_half <- cut4(sc_half)
cat(sprintf("ANY 4-bucket change: %d of %d (%.3f%%)\n",
            sum(b_ann != b_half), nrow(st), 100 * mean(b_ann != b_half)))
cat("\ngood-start rate by live 4-bucket, annual vs half PF:\n")
gr <- function(b) tapply(st$good_start, b, mean)
print(round(cbind(annual = gr(b_ann), half = gr(b_half),
                  delta_pp = 100 * (gr(b_half) - gr(b_ann))), 4))
cat("\nbucket sizes:\n")
print(rbind(annual = table(b_ann), half = table(b_half)))
cat("\n")

# ── Pooled + per-season scoreboard ───────────────────────────────────────────
arms <- list(`annual-overall` = sc_ann, `half-overall` = sc_half,
             `annual-bacon` = sc_annB,  `half-bacon` = sc_halfB)
pooled <- t(vapply(arms, scoreboard, numeric(4)))
cat("--- Pooled scoreboard ---\n")
print(round(pooled, 5))

per_season <- lapply(arms, function(s)
  t(vapply(SEASONS, function(yr) scoreboard(s, st$season == yr), numeric(4))))
dir_count <- function(a, b, metric, better_lower) {
  d <- per_season[[b]][, metric] - per_season[[a]][, metric]
  if (better_lower) sum(d < 0) else sum(d > 0)
}
cat("\n--- Seasons (of 5) where the half arm is better ---\n")
for (m in c("M1q", "M2q", "M3q", "rho")) {
  cat(sprintf("  %s  overall %d/5 | bacon %d/5\n", m,
              dir_count("annual-overall", "half-overall", m, m == "M1q"),
              dir_count("annual-bacon",   "half-bacon",   m, m == "M1q")))
}

cat("\n--- Per-season M1q (ER5+ rate in the start zone, lower better) ---\n")
m1 <- data.frame(season = SEASONS,
                 annual = per_season[["annual-overall"]][, "M1q"],
                 half   = per_season[["half-overall"]][, "M1q"])
m1$delta <- m1$half - m1$annual
print(m1, row.names = FALSE, digits = 4)

# ── Paired bootstrap on the pooled deltas ────────────────────────────────────
boot_delta <- function(sa, sb, B = 2000) {
  n <- nrow(st)
  t(replicate(B, {
    i <- sample.int(n, n, replace = TRUE)
    a <- sa[i]; b <- sb[i]
    blow <- st$blowup_er5[i]; good <- st$good_start[i]; gsm <- st$gsm[i]
    hi_a <- a >= quantile(a, 1 - TOP_SHARE); hi_b <- b >= quantile(b, 1 - TOP_SHARE)
    lo_a <- a <= quantile(a, BOT_SHARE);     lo_b <- b <= quantile(b, BOT_SHARE)
    c(M1q = mean(blow[hi_b]) - mean(blow[hi_a]),
      M2q = sum(blow & lo_b) / sum(blow) - sum(blow & lo_a) / sum(blow),
      M3q = mean(good[hi_b]) - mean(good[hi_a]),
      rho = cor(b, gsm, method = "spearman") - cor(a, gsm, method = "spearman"))
  }))
}
cat("\n--- Paired bootstrap, half minus annual (B=2000) ---\n")
for (pair in list(c("annual-overall", "half-overall"), c("annual-bacon", "half-bacon"))) {
  bd <- boot_delta(arms[[pair[1]]], arms[[pair[2]]])
  cat(sprintf("\n%s -> %s\n", pair[1], pair[2]))
  ci <- t(apply(bd, 2, quantile, c(0.025, 0.5, 0.975)))
  obs <- pooled[pair[2], ] - pooled[pair[1], ]
  print(round(cbind(observed = obs, ci), 5))
}

# ── Verdict on the pre-registered rule (M1q, Overall lens) ───────────────────
rel <- (pooled["half-overall", "M1q"] - pooled["annual-overall", "M1q"]) /
        pooled["annual-overall", "M1q"]
cat(sprintf("\n--- Verdict ---\nM1q relative change: %+.2f%% (rule needs <= -2%%)\n", 100 * rel))

res <- data.frame(arm = rownames(pooled), pooled, row.names = NULL)
write.csv(res, OUT, row.names = FALSE)
cat(sprintf("Wrote %s\n", OUT))
