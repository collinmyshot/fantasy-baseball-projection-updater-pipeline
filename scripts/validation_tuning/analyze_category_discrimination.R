#!/usr/bin/env Rscript
# analyze_category_discrimination.R
# ---------------------------------------------------------------------------
# FAIR cross-category comparison: each category evaluated on ITS OWN relevant
# population (top-N projected in that category per week), gap-conditioned, and
# split by tier (established vs marginal) to see whether the marginal tier is
# genuinely harder to predict. This replaces the earlier apples-to-oranges
# "SB-among-runners vs other-cats-whole-pool" comparison.
# gap = 10  (the realistic "top-10 vs top-20" streaming choice). 50% = coin flip.
# ---------------------------------------------------------------------------
suppressWarnings(suppressMessages(library(dplyr)))
ROOT <- "/Users/ckaufman/Documents/New project"
f <- read.csv(file.path(ROOT, "data/processed/hitter_week_features.csv"), stringsAsFactors = FALSE)
f <- f %>% filter(has_steamer, is.finite(value_5x5_proj))

gap_acc <- function(df, proj, real, g) {
  wk <- split(df[, c(proj, real)], df$week_start); num<-0; den<-0; tie<-0
  for (w in wk) {
    o <- order(-w[[proj]]); rv <- w[[real]][o]; n <- length(rv)
    if (n > g) { hi <- rv[1:(n-g)]; lo <- rv[(g+1):n]
      num <- num + sum(hi>lo) + 0.5*sum(hi==lo); tie <- tie + sum(hi==lo); den <- den + length(hi) }
  }
  c(acc = 100*num/den, tie = 100*tie/den)
}

cats <- list(SB=c("sb_proj","sb"), HR=c("hr_proj","hr"), R=c("r_proj","r"),
             RBI=c("rbi_proj","rbi"), AVG=c("avgval_proj","avg_val"))
TOPN <- 120; GAP <- 10

res <- data.frame()
for (cat in names(cats)) {
  pc <- cats[[cat]][1]; rc <- cats[[cat]][2]
  d <- f %>% group_by(week_start) %>% mutate(rk = rank(-.data[[pc]], ties.method="first")) %>%
    filter(rk <= TOPN) %>% ungroup()
  for (tr in c("ALL","established","marginal")) {
    dd <- if (tr == "ALL") d else d[d$tier == tr, ]
    if (nrow(dd) < 800) next
    ga <- gap_acc(dd, pc, rc, GAP)
    res <- rbind(res, data.frame(category=cat, tier=tr, n=nrow(dd),
                                 acc_gap10=round(ga["acc"],1), tie_pct=round(ga["tie"],1)))
  }
}
cat(sprintf("Top-%d projected per category/week; pick accuracy at gap=%d (top-10 vs top-20). 50%%=coin flip.\n\n", TOPN, GAP))
print(res, row.names = FALSE)
