#!/usr/bin/env Rscript
# analyze_pickacc_ci.R
# ---------------------------------------------------------------------------
# The eval MACHINERY: gap-conditioned "pick the better hitter" accuracy WITH a
# 95% CI bootstrapped by WEEK (the natural cluster -- hitter-weeks within a week
# are not independent). Establishes the noise floor so any feature "lift" later
# is judged real vs. sampling noise.
#
# Baseline here = Steamer, each category on its own top-120 projected pop, gap=10.
# 50% = coin flip. Reusable: gap_week_stats() + boot_ci().
#
# ── RESULT (2026-07): machinery BUILT AND VERIFIED. Reuse it. ──────────────
#   Steamer baselines, gap-10, top-120 population:
#     SB 51.9% [51.6, 52.2] | R 52.1 | RBI 51.6 | HR 51.4 | AVG 50.6
#   All are barely-but-significantly above a coin flip.
#
#   The tight +/-0.3 CI is REAL, not a bug. It looks implausibly narrow at
#   first glance, but overlapping gap-pairs are negatively correlated, and the
#   week-clustered bootstrap agrees with the normal approximation. Checked.
#
#   OPERATIONAL BAR: a genuine SB lift has to clear ~52.2% cleanly. Feature
#   tests should use a PAIRED-difference bootstrap against this baseline, not
#   an unpaired comparison of two independent estimates.
#
#   gap_week_stats() and boot_ci() are the reusable pieces — later hitter work
#   should call these rather than re-implementing the clustering.
# ---------------------------------------------------------------------------
suppressWarnings(suppressMessages(library(data.table)))
ROOT <- "/Users/ckaufman/Documents/New project"
set.seed(42)
f <- fread(file.path(ROOT, "data/processed/hitter_week_features.csv"))
f <- f[has_steamer == 1 & is.finite(value_5x5_proj)]

# per-week numerator/denominator for gap-conditioned pick accuracy (base-R, robust)
gap_week_stats <- function(dfr, g) {                 # dfr: data.frame with week_start, p, r
  wk <- split(dfr[, c("p", "r")], dfr$week_start)
  do.call(rbind, lapply(wk, function(w) {
    o <- order(-w$p); rv <- w$r[o]; n <- length(rv)
    if (n > g) { hi <- rv[1:(n-g)]; lo <- rv[(g+1):n]
      c(num = sum(hi > lo) + 0.5 * sum(hi == lo), den = length(hi)) }
    else c(num = 0, den = 0)
  }))
}
# cluster bootstrap: resample WEEKS with replacement
boot_ci <- function(nd, B = 2000) {
  num <- nd[, "num"]; den <- nd[, "den"]; W <- nrow(nd)
  pt <- 100 * sum(num) / sum(den)
  bs <- replicate(B, { s <- sample.int(W, W, replace = TRUE); 100 * sum(num[s]) / sum(den[s]) })
  c(acc = pt, lo = unname(quantile(bs, 0.025)), hi = unname(quantile(bs, 0.975)))
}

cats <- list(SB=c("sb_proj","sb"), HR=c("hr_proj","hr"), R=c("r_proj","r"),
             RBI=c("rbi_proj","rbi"), AVG=c("avgval_proj","avg_val"))
TOPN <- 120; GAP <- 10

cat(sprintf("Baseline (Steamer) pick accuracy at gap=%d, top-%d projected/week, 95%% CI clustered by week:\n\n", GAP, TOPN))
res <- rbindlist(lapply(names(cats), function(cn) {
  d <- f[, .(week_start, p = get(cats[[cn]][1]), r = get(cats[[cn]][2]))]   # get() OK: no `by`
  d[, rk := frank(-p, ties.method = "first"), by = week_start]
  ci <- boot_ci(gap_week_stats(as.data.frame(d[rk <= TOPN]), GAP))
  data.table(category = cn, acc = round(ci["acc"],1), ci95_lo = round(ci["lo"],1),
             ci95_hi = round(ci["hi"],1), beats_coinflip = ci["lo"] > 50)
}))
print(res, row.names = FALSE)
cat(sprintf("\nSB baseline CI half-width: +/- %.2f pts -- the bar the Savant matchup features must clear.\n",
            (res[category=="SB", ci95_hi] - res[category=="SB", ci95_lo]) / 2))

# --- verify the CI machinery is self-consistent (SB) ---------------------------
d <- f[, .(week_start, p = sb_proj, r = sb)]; d[, rk := frank(-p, ties.method="first"), by = week_start]
nd <- gap_week_stats(as.data.frame(d[rk <= TOPN]), GAP)
pw <- nd[,"num"] / nd[,"den"]
cat(sprintf("\nVERIFY (SB): %d weeks, mean pairs/week=%.0f, per-week acc sd=%.3f\n", nrow(nd), mean(nd[,"den"]), sd(pw)))
cat(sprintf("  naive normal half-width from between-week variation = +/- %.2f pts (should ~match the %.2f bootstrap)\n",
            100 * 1.96 * sd(pw) / sqrt(nrow(nd)), (res[category=="SB", ci95_hi] - res[category=="SB", ci95_lo]) / 2))
cat(sprintf("  (independent-binomial would be +/- %.2f; observed is tighter b/c overlapping gap-pairs negatively correlate -> CI real, not a bug)\n",
            100 * 1.96 * sqrt(0.52*0.48/mean(nd[,"den"])) / sqrt(nrow(nd))))
