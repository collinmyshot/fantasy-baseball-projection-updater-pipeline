#!/usr/bin/env Rscript
# qa_spine_reconcile.R
# ---------------------------------------------------------------------------
# Step 4: end-to-end spine QA (never done). Reconcile our 2024 spine HR/SB
# totals against MLB's OFFICIAL league totals (MLB Stats API team season
# hitting, summed) -- catches dropped games, double-counts, parse drift.
# Also quantifies the pitcher-batting rows (should be ~0 HR/SB in 2024).
# ---------------------------------------------------------------------------
suppressWarnings(suppressMessages({library(data.table); library(jsonlite)}))
options(timeout = 120)
ROOT <- "/Users/ckaufman/Documents/New project"

gl <- fread(file.path(ROOT,"data/processed/hitter_game_logs/hitter_game_logs_2024-03-01_2024-11-30.csv"),
            select=c("game_pk","person_id","position","hr","sb"))
gl <- unique(gl, by=c("game_pk","person_id"))
n0 <- function(x) fifelse(is.na(x),0L,as.integer(x))
allh <- gl[, .(hr=sum(n0(hr)), sb=sum(n0(sb)))]
nonp <- gl[is.na(position) | position!="P", .(hr=sum(n0(hr)), sb=sum(n0(sb)))]
pit  <- gl[position=="P", .(rows=.N, hr=sum(n0(hr)), sb=sum(n0(sb)))]

# official reference: sum team season hitting from MLB Stats API
ref <- tryCatch(fromJSON("https://statsapi.mlb.com/api/v1/teams/stats?stats=season&group=hitting&season=2024&sportId=1&gameType=R",
                         simplifyVector=FALSE), error=function(e) NULL)
ref_hr <- ref_sb <- NA
if (!is.null(ref)) {
  sp <- ref$stats[[1]]$splits
  ref_hr <- sum(vapply(sp, function(s) as.integer(s$stat$homeRuns), integer(1)))
  ref_sb <- sum(vapply(sp, function(s) as.integer(s$stat$stolenBases), integer(1)))
}

cat("2024 spine reconciliation (regular season):\n")
cat(sprintf("  spine, all rows        : HR=%d  SB=%d\n", allh$hr, allh$sb))
cat(sprintf("  spine, non-pitchers    : HR=%d  SB=%d\n", nonp$hr, nonp$sb))
cat(sprintf("  pitcher-batting rows   : n=%d  HR=%d  SB=%d\n", pit$rows, pit$hr, pit$sb))
cat(sprintf("  MLB official (Stats API): HR=%s  SB=%s  (%d teams summed)\n",
            ref_hr, ref_sb, if (!is.null(ref)) length(ref$stats[[1]]$splits) else 0L))
if (!is.na(ref_hr)) cat(sprintf("  DIFF (spine all - official): HR=%+d (%.2f%%)  SB=%+d (%.2f%%)\n",
    allh$hr-ref_hr, 100*(allh$hr-ref_hr)/ref_hr, allh$sb-ref_sb, 100*(allh$sb-ref_sb)/ref_sb))
