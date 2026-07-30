#!/usr/bin/env Rscript
# bucket_analysis.R
#
# Loads cached starts_YYYY.csv files (2021–2025) and runs two analyses:
#
#  Analysis A — Tightened bucket grid search (±5 threshold)
#    Buckets: >105 = "start", <95 = "sit", 95–105 = "flip"
#    Grid search (66 combos) run independently within each bucket.
#    Reports: Spearman rho and pct_ge3 (top quartile ≥3 good-start score).
#
#  Analysis B — Flipped distribution (outcome → score)
#    For each good_start outcome tier (≤1, ≥4), show the distribution of
#    stream scores under the reference 6:3:1 weighting.
#    Answers: "Do good starts tend to come from high predictions?"
#
# ── WHY THIS FILE MATTERS ───────────────────────────────────────────────────
#   This is the script that RECOMPUTES the authoritative GSM (0-4: no Win,
#   sliding ER cap, WHIP <= 1.18) rather than trusting the stale 0-5
#   good_start_score column baked into starts_YYYY.csv by the derive_* scripts.
#   If you are looking for "the actual bucket validation", it is this one.
#   Any new script reading starts_YYYY.csv must recompute GSM the same way.
#
# ── NOTE ON SCORING VINTAGE ─────────────────────────────────────────────────
#   Runs on SEASON-FINAL scoring (hindsight), which is fine for comparing
#   weight combos but inflates absolute bucket rates. The point-in-time
#   equivalent — and the source of the published appendix figures — is
#   fbb-tools scripts/build_stream_calibration.R. For the 95-105 band
#   specifically, streamonator_coinflip_reweight.R supersedes Analysis A here:
#   same question, point-in-time data, LOSO CV and clustered bootstrap.
#
# All data is fully cached — no API calls made.
# Usage: Rscript scripts/validation_tuning/bucket_analysis.R

CACHE_DIR <- file.path("data", "processed", "streamonator_weight_analysis")
SEASONS   <- 2021:2025

# ── Load + combine all starts CSVs ────────────────────────────────────────────
message("Loading starts CSVs...")
all_starts <- do.call(rbind, lapply(SEASONS, function(yr) {
  f <- file.path(CACHE_DIR, sprintf("starts_%d.csv", yr))
  if (!file.exists(f)) { message(sprintf("  MISSING: %s — skipping", f)); return(NULL) }
  df <- read.csv(f, stringsAsFactors = FALSE)
  df$season <- yr
  message(sprintf("  %d: %d rows", yr, nrow(df)))
  df
}))
message(sprintf("Combined: %d starts across %d seasons\n", nrow(all_starts), length(SEASONS)))

# ── Recompute good_start_score under new criteria (0–4, no Win) ───────────────
# Criteria:
#   IP    : >= 5.0
#   K     : K >= floor(IP) - 1
#   ER    : sliding scale by IP — >=6 IP: <=2 ER; 5.x IP: <=3 ER;
#           4.x IP: <=2 ER; <4 IP: <=1 ER
#   WHIP  : <= 1.18
# Good start = 3-4  |  OK = 2  |  Bad = 0-1
message("Recomputing good_start_score (new criteria: no Win, WHIP<=1.18, sliding ER)...")
all_starts$whip <- ifelse(
  !is.na(all_starts$ip) & all_starts$ip > 0,
  (all_starts$h + all_starts$bb) / all_starts$ip, Inf)

all_starts$ip_ok <- !is.na(all_starts$ip) & all_starts$ip >= 5.0

all_starts$k_ok  <- !is.na(all_starts$k) & !is.na(all_starts$ip) &
                    all_starts$k >= (floor(all_starts$ip) - 1)

all_starts$er_ok <- !is.na(all_starts$er) & !is.na(all_starts$ip) & (
  (all_starts$ip >= 6.0                              & all_starts$er <= 2) |
  (all_starts$ip >= 5.0 & all_starts$ip < 6.0       & all_starts$er <= 3) |
  (all_starts$ip >= 4.0 & all_starts$ip < 5.0       & all_starts$er <= 2) |
  (all_starts$ip <  4.0                              & all_starts$er <= 1)
)

all_starts$whip_ok <- !is.na(all_starts$whip) & all_starts$whip <= 1.18

all_starts$good_start_score <-
  as.integer(all_starts$ip_ok)   +
  as.integer(all_starts$k_ok)    +
  as.integer(all_starts$er_ok)   +
  as.integer(all_starts$whip_ok)

score_dist <- table(all_starts$good_start_score)
message("  Score distribution (0=bad, 4=great):")
for (sc in names(score_dist)) {
  pct <- round(100 * score_dist[[sc]] / nrow(all_starts), 1)
  message(sprintf("    %s: %d (%.1f%%)", sc, score_dist[[sc]], pct))
}
message(sprintf("  Good (3-4): %.1f%%  |  OK (2): %.1f%%  |  Bad (0-1): %.1f%%\n",
  100 * mean(all_starts$good_start_score >= 3, na.rm=TRUE),
  100 * mean(all_starts$good_start_score == 2, na.rm=TRUE),
  100 * mean(all_starts$good_start_score <= 1, na.rm=TRUE)))

# ── Reference stream score (6:3:1) ────────────────────────────────────────────
ref_sp <- 6; ref_tr <- 3; ref_pf <- 1
all_starts$stream_score_ref <- vapply(seq_len(nrow(all_starts)), function(i) {
  sp  <- all_starts$sp_skillz_index[i]
  tr  <- all_starts$team_rater_inv[i]
  pf  <- all_starts$park_factor_inv[i]
  wts <- c(ref_sp, ref_tr, ref_pf)
  vals <- c(sp, tr, pf)
  ok  <- !is.na(vals) & wts > 0
  if (!any(ok)) return(NA_real_)
  sum(vals[ok] * wts[ok]) / sum(wts[ok])
}, numeric(1))

# ── Grid template (66 combos summing to 1) ────────────────────────────────────
grid_raw <- expand.grid(
  w_sp = seq(0, 1, by = 0.1),
  w_tr = seq(0, 1, by = 0.1),
  w_pf = seq(0, 1, by = 0.1)
)
GRID <- grid_raw[abs(rowSums(grid_raw) - 1) < 1e-9, , drop = FALSE]
rownames(GRID) <- NULL

# ── Grid search helper ─────────────────────────────────────────────────────────
run_grid <- function(df, label) {
  outcome <- df$good_start_score
  results <- do.call(rbind, lapply(seq_len(nrow(GRID)), function(j) {
    w_sp <- GRID$w_sp[j]; w_tr <- GRID$w_tr[j]; w_pf <- GRID$w_pf[j]
    scores <- vapply(seq_len(nrow(df)), function(i) {
      vals <- c(df$sp_skillz_index[i], df$team_rater_inv[i], df$park_factor_inv[i])
      wts  <- c(w_sp, w_tr, w_pf)
      ok   <- !is.na(vals) & wts > 0
      if (!any(ok)) return(NA_real_)
      sum(vals[ok] * wts[ok]) / sum(wts[ok])
    }, numeric(1))
    valid <- !is.na(scores) & !is.na(outcome)
    if (sum(valid) < 10L) return(NULL)
    rho     <- suppressWarnings(cor(scores[valid], outcome[valid], method = "spearman"))
    top_cut <- quantile(scores[valid], 0.75, na.rm = TRUE)
    pct_ge3 <- mean(outcome[valid & scores >= top_cut] >= 3, na.rm = TRUE)
    data.frame(bucket = label, w_sp = w_sp, w_tr = w_tr, w_pf = w_pf,
               spearman = round(rho, 4), pct_ge3 = round(pct_ge3, 4),
               n = sum(valid), stringsAsFactors = FALSE)
  }))
  results[order(-results$spearman), , drop = FALSE]
}

# ══════════════════════════════════════════════════════════════════════════════
# ANALYSIS A — Tightened bucket grid search (thresholds: 95 / 105)
# ══════════════════════════════════════════════════════════════════════════════
message("════════════════════════════════════════════════════════════")
message("  ANALYSIS A — Tightened buckets (95 / 105 thresholds)")
message("════════════════════════════════════════════════════════════\n")

valid_ref <- !is.na(all_starts$stream_score_ref)
all_starts$bucket_tight <- NA_character_
all_starts$bucket_tight[valid_ref & all_starts$stream_score_ref >  105] <- "start"
all_starts$bucket_tight[valid_ref & all_starts$stream_score_ref <   95] <- "sit"
all_starts$bucket_tight[valid_ref & all_starts$stream_score_ref >= 95 &
                          all_starts$stream_score_ref <= 105]            <- "flip"

bucket_ns <- table(all_starts$bucket_tight)
message(sprintf("  Bucket sizes  (threshold 95 / 105):"))
message(sprintf("    Obvious start  (>105):  n = %d  (%.1f%%)",
  bucket_ns["start"], 100 * bucket_ns["start"] / sum(bucket_ns)))
message(sprintf("    Coin flip  (95–105):    n = %d  (%.1f%%)",
  bucket_ns["flip"],  100 * bucket_ns["flip"]  / sum(bucket_ns)))
message(sprintf("    Obvious sit   (<95):    n = %d  (%.1f%%)\n",
  bucket_ns["sit"],   100 * bucket_ns["sit"]   / sum(bucket_ns)))

# Compare to old thresholds for context
old_start <- sum(valid_ref & all_starts$stream_score_ref > 110, na.rm = TRUE)
old_flip  <- sum(valid_ref & all_starts$stream_score_ref >= 90 & all_starts$stream_score_ref <= 110, na.rm = TRUE)
old_sit   <- sum(valid_ref & all_starts$stream_score_ref < 90, na.rm = TRUE)
message("  (Old 90/110 thresholds for comparison:)")
message(sprintf("    start >110: n=%d  flip 90-110: n=%d  sit <90: n=%d)\n",
  old_start, old_flip, old_sit))

bucket_results_A <- list()
for (bkt in c("start", "flip", "sit")) {
  sub <- all_starts[!is.na(all_starts$bucket_tight) & all_starts$bucket_tight == bkt, ]
  message(sprintf("  Running grid search — %s bucket (n=%d)...", bkt, nrow(sub)))
  gr <- run_grid(sub, bkt)
  bucket_results_A[[bkt]] <- gr
}

# Overall (all starts)
message(sprintf("  Running grid search — Overall (n=%d)...", sum(valid_ref)))
gr_all <- run_grid(all_starts[valid_ref, ], "overall")
bucket_results_A[["overall"]] <- gr_all

message("\n  ── RESULTS (top combo per bucket) ─────────────────────────────────\n")
message(sprintf("  %-10s  %5s  %5s  %5s  %8s  %8s  %7s",
  "Bucket", "w_sp", "w_tr", "w_pf", "Spearman", "pct_ge3", "N"))
message(sprintf("  %s", strrep("-", 60)))

for (bkt in c("overall", "start", "flip", "sit")) {
  gr  <- bucket_results_A[[bkt]]
  top <- gr[1, ]
  message(sprintf("  %-10s  %5.1f  %5.1f  %5.1f  %8.4f  %8.4f  %7d",
    bkt, top$w_sp, top$w_tr, top$w_pf, top$spearman, top$pct_ge3, top$n))
}

# Show reference (6:3:1) row for each bucket too
message(sprintf("\n  ── Reference weights 6:3:1 (normalized: 0.6/0.3/0.1) ─────────\n"))
message(sprintf("  %-10s  %5s  %5s  %5s  %8s  %8s  %7s",
  "Bucket", "w_sp", "w_tr", "w_pf", "Spearman", "pct_ge3", "N"))
message(sprintf("  %s", strrep("-", 60)))
for (bkt in c("overall", "start", "flip", "sit")) {
  gr  <- bucket_results_A[[bkt]]
  ref_row <- gr[abs(gr$w_sp - 0.6) < 0.05 & abs(gr$w_tr - 0.3) < 0.05 & abs(gr$w_pf - 0.1) < 0.05, ]
  if (nrow(ref_row) == 0) ref_row <- gr[abs(gr$w_sp - 0.6) < 0.06 & abs(gr$w_tr - 0.3) < 0.06, ][1, ]
  if (nrow(ref_row) == 0 || is.na(ref_row$spearman[1])) {
    message(sprintf("  %-10s  [6:3:1 row not found in grid]", bkt)); next
  }
  top <- bucket_results_A[[bkt]][1, ]
  delta <- round(top$spearman - ref_row$spearman[1], 4)
  message(sprintf("  %-10s  %5.1f  %5.1f  %5.1f  %8.4f  %8.4f  %7d  (Δ vs best: %+.4f)",
    bkt, ref_row$w_sp[1], ref_row$w_tr[1], ref_row$w_pf[1],
    ref_row$spearman[1], ref_row$pct_ge3[1], ref_row$n[1], delta))
}

# Show top 5 for flip bucket specifically (most decision-relevant)
message("\n  ── Top 10 combos — FLIP bucket (the decision zone) ────────────────\n")
flip_gr <- bucket_results_A[["flip"]]
message(sprintf("  %5s  %5s  %5s  %8s  %8s", "w_sp", "w_tr", "w_pf", "Spearman", "pct_ge3"))
message(sprintf("  %s", strrep("-", 42)))
for (i in seq_len(min(10, nrow(flip_gr)))) {
  r <- flip_gr[i, ]
  message(sprintf("  %5.1f  %5.1f  %5.1f  %8.4f  %8.4f", r$w_sp, r$w_tr, r$w_pf, r$spearman, r$pct_ge3))
}

# ══════════════════════════════════════════════════════════════════════════════
# ANALYSIS B — Flipped: stream score distribution by outcome tier
# ══════════════════════════════════════════════════════════════════════════════
message("\n\n════════════════════════════════════════════════════════════")
message("  ANALYSIS B — Flipped: stream score by good_start outcome")
message("════════════════════════════════════════════════════════════\n")

df_valid <- all_starts[valid_ref & !is.na(all_starts$good_start_score), ]
n_total  <- nrow(df_valid)

# Define outcome tiers
df_valid$outcome_tier <- ifelse(df_valid$good_start_score >= 3, "good (3-4)",
                         ifelse(df_valid$good_start_score <= 1, "bad (0-1)",
                                "ok (2)"))

# Stream score quintile bins (using all starts as reference)
breaks <- quantile(df_valid$stream_score_ref, probs = seq(0, 1, 0.2), na.rm = TRUE)
breaks[1] <- breaks[1] - 0.001  # ensure lowest value is included
df_valid$score_quintile <- cut(df_valid$stream_score_ref, breaks = breaks,
  labels = c("Q1 (lowest 20%)", "Q2", "Q3", "Q4", "Q5 (top 20%)"),
  include.lowest = TRUE)

message("  Stream score distribution within each outcome tier (6:3:1 weights)\n")

# Header
message(sprintf("  %-14s  %6s  %8s  %8s  %8s  %8s  %8s",
  "Outcome Tier", "N", "Min", "Q25", "Median", "Q75", "Max"))
message(sprintf("  %s", strrep("-", 72)))

for (tier in c("good (3-4)", "ok (2)", "bad (0-1)")) {
  sub <- df_valid$stream_score_ref[df_valid$outcome_tier == tier]
  if (!length(sub)) next
  qs <- quantile(sub, probs = c(0, 0.25, 0.5, 0.75, 1), na.rm = TRUE)
  message(sprintf("  %-14s  %6d  %8.1f  %8.1f  %8.1f  %8.1f  %8.1f",
    tier, length(sub), qs[1], qs[2], qs[3], qs[4], qs[5]))
}

# Score quintile breakdown — what fraction of each tier falls in each quintile?
message("\n  Stream score quintile breakdown by outcome tier\n")
message(sprintf("  %-14s  %16s  %16s  %16s  %16s  %16s",
  "Outcome Tier", "Q1 (bot 20%)", "Q2", "Q3", "Q4", "Q5 (top 20%)"))
message(sprintf("  %s", strrep("-", 95)))

for (tier in c("good (3-4)", "ok (2)", "bad (0-1)")) {
  sub  <- df_valid[df_valid$outcome_tier == tier, ]
  n    <- nrow(sub)
  if (!n) next
  qtbl <- table(sub$score_quintile)
  pcts <- sapply(c("Q1 (lowest 20%)", "Q2", "Q3", "Q4", "Q5 (top 20%)"), function(q) {
    cnt <- if (!is.null(qtbl[q]) && !is.na(qtbl[q])) qtbl[q] else 0L
    sprintf("%4d (%5.1f%%)", cnt, 100 * cnt / n)
  })
  message(sprintf("  %-14s  %16s  %16s  %16s  %16s  %16s",
    tier, pcts[1], pcts[2], pcts[3], pcts[4], pcts[5]))
}

# Recall / precision table
message("\n  Discrimination summary — how well does stream score separate great from bad?\n")

great_scores <- df_valid$stream_score_ref[df_valid$outcome_tier == "good (3-4)"]
bad_scores   <- df_valid$stream_score_ref[df_valid$outcome_tier == "bad (0-1)"]

mean_great <- mean(great_scores, na.rm = TRUE)
mean_bad   <- mean(bad_scores,   na.rm = TRUE)
med_great  <- median(great_scores, na.rm = TRUE)
med_bad    <- median(bad_scores,   na.rm = TRUE)

message(sprintf("  Mean stream score — great (4-5): %.2f   bad (0-1): %.2f   Δ = %.2f",
  mean_great, mean_bad, mean_great - mean_bad))
message(sprintf("  Median stream score — great (4-5): %.2f   bad (0-1): %.2f   Δ = %.2f",
  med_great, med_bad, med_great - med_bad))

# Top-quartile recall: of all great (4-5) starts, what % were in the top 25% of stream scores?
top_cut_all <- quantile(df_valid$stream_score_ref, 0.75, na.rm = TRUE)
pct_great_in_top <- mean(great_scores >= top_cut_all, na.rm = TRUE)
pct_bad_in_top   <- mean(bad_scores   >= top_cut_all, na.rm = TRUE)
message(sprintf("\n  Of great (4-5) starts:    %.1f%% had stream score in top 25%%",
  100 * pct_great_in_top))
message(sprintf("  Of bad (0-1) starts:      %.1f%% had stream score in top 25%%",
  100 * pct_bad_in_top))
message(sprintf("  Lift (great vs bad in top quartile): %.2fx", pct_great_in_top / pct_bad_in_top))

# Bottom-quartile: of bad (0-1) starts, what % were in the bottom 25%?
bot_cut_all <- quantile(df_valid$stream_score_ref, 0.25, na.rm = TRUE)
pct_bad_in_bot   <- mean(bad_scores   <= bot_cut_all, na.rm = TRUE)
pct_great_in_bot <- mean(great_scores <= bot_cut_all, na.rm = TRUE)
message(sprintf("\n  Of bad (0-1) starts:      %.1f%% had stream score in bottom 25%%",
  100 * pct_bad_in_bot))
message(sprintf("  Of great (4-5) starts:    %.1f%% had stream score in bottom 25%%",
  100 * pct_great_in_bot))

# AUC-like: rank-biserial correlation (Wilcoxon) between stream score and binary great/not-great
y_binary <- as.integer(df_valid$outcome_tier == "good (3-4)")
w_test <- suppressWarnings(wilcox.test(
  df_valid$stream_score_ref[y_binary == 1],
  df_valid$stream_score_ref[y_binary == 0],
  alternative = "greater"
))
n1 <- sum(y_binary == 1); n0 <- sum(y_binary == 0)
auc <- w_test$statistic / (n1 * n0)
message(sprintf("\n  AUC (good ≥3 vs bad ≤1): %.4f  (0.5 = random; 1.0 = perfect)", auc))

message("\n════════════════════════════════════════════════════════════")
message("  Done.")
message("════════════════════════════════════════════════════════════")
