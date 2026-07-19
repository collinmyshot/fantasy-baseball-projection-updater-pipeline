#!/usr/bin/env Rscript
# analyze_baseline_discrimination.R
# ---------------------------------------------------------------------------
# Honest "pick the better hitter" accuracy for the Steamer baseline.
#
# Two fixes vs the naive all-pairs number:
#  (1) condition on the projected-RANK gap (adjacent = the realistic choice).
#  (2) evaluate each category on its CATEGORY-RELEVANT population -- for SB,
#      the projected base-stealers (comparing two non-runners on steals is
#      meaningless and just manufactures ties).
# 50% = coin flip.
# ---------------------------------------------------------------------------
suppressWarnings(suppressMessages(library(dplyr)))
ROOT <- "/Users/ckaufman/Documents/New project"
f <- read.csv(file.path(ROOT, "data/processed/hitter_week_features.csv"), stringsAsFactors = FALSE)
f <- f %>% filter(has_steamer, is.finite(value_5x5_proj))

# within-week accuracy comparing hitter ranked i (higher proj) to i+gap (lower)
gap_acc <- function(df, proj, real, gaps) {
  wk <- split(df[, c(proj, real)], df$week_start)
  sapply(gaps, function(g) {
    num <- 0; den <- 0; tie <- 0
    for (w in wk) {
      o <- order(-w[[proj]]); rv <- w[[real]][o]; n <- length(rv)
      if (n > g) {
        hi <- rv[1:(n - g)]; lo <- rv[(g + 1):n]
        num <- num + sum(hi > lo) + 0.5 * sum(hi == lo)
        tie <- tie + sum(hi == lo); den <- den + length(hi)
      }
    }
    c(acc = 100 * num / den, tie = 100 * tie / den)
  })
}
gaps <- c(1, 5, 10)

cat("=== SB: whole full-time pool (WRONG population -- most don't run) ===\n")
sb_all <- gap_acc(f, "sb_proj", "sb", gaps)
print(data.frame(gap = gaps, SB_acc = round(sb_all["acc",],1), tie_pct = round(sb_all["tie",],1)), row.names = FALSE)

cat("\n=== SB: restricted to the projected-RUNNER tier (the tool's real population) ===\n")
for (N in c(30, 60, 90)) {
  d <- f %>% group_by(week_start) %>%
    mutate(sb_rank = rank(-sb_proj, ties.method = "first")) %>%
    filter(sb_rank <= N) %>% ungroup()
  ga <- gap_acc(d, "sb_proj", "sb", gaps)
  cat(sprintf("top %d projected base-stealers/week (n=%d hitter-weeks):\n", N, nrow(d)))
  print(data.frame(gap = gaps, SB_acc = round(ga["acc",],1), tie_pct = round(ga["tie",],1)), row.names = FALSE)
  cat("\n")
}

# what does weekly SB actually look like among the top-60 projected runners?
d60 <- f %>% group_by(week_start) %>% mutate(sb_rank = rank(-sb_proj, ties.method="first")) %>%
  filter(sb_rank <= 60) %>% ungroup()
cat("Realized weekly SB distribution, top-60 projected runners:\n")
print(round(100 * prop.table(table(pmin(d60$sb, 4))), 1))
cat("(columns = 0,1,2,3,4+ steals in the week)\n")
