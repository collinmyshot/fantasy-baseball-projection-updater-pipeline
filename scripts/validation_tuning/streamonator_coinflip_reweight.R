#!/usr/bin/env Rscript
# ══════════════════════════════════════════════════════════════════════════════
# streamonator_coinflip_reweight.R
#
# QUESTION
#   6:3:1 (SP Skillz : Team Rater : Park Factor) is the best single weighting
#   across the whole population of starts. But the tool's job in the Coin Flip
#   band (95 < score < 105) is different: those starts are already "we don't
#   know". So — take a 6:3:1 pass, isolate the Coin Flip starts, and ask whether
#   a DIFFERENT weight combination re-sorts THOSE starts better on
#     success = GSM >= 3
#     bust    = ER >= 5 OR WHIP >= 2.05
#
#   This is a two-stage decision rule, not a replacement for 6:3:1:
#     Stage 1  score 6:3:1 -> bucket (this is what ships today)
#     Stage 2  inside the Coin Flip bucket only, re-rank on challenger weights
#   The Auto-Start / Auto-Bench buckets are untouched by construction.
#
# ── ANSWER, 2026-07-30 (2021-2025, n = 10,135 in band): NO. REJECT x4. ───────
#   Already run. Do not re-litigate this without a NEW input signal — re-mixing
#   the same three components has been tested to exhaustion at 0.05 resolution.
#
#   6:3:1 ranks 11 / 10 / 17 / 13 out of 231 combos on AUC-success, AUC-bust,
#   success-tercile-spread, bust-tercile-spread. Best in-sample gain is trivial
#   (AUC success .5576 -> .5604, AUC bust .5450 -> .5478) and does not survive:
#     LOSO mean OOS delta  +0.6% / -5.9% / -0.3% / -14.6%
#     seasons improved      3/5  /  1/5  /  2/5  /   1/5
#     bootstrap CI excl 0    no  /   no  /   no  /    no    (and that CI is
#       optimistic — the challenger was picked on the same data)
#   Frozen challengers (6:2:2, 6.5:2:1.5, 5.5:2.5:2, 6:1.5:2.5, 7:2:1) all fail
#   too; none clears 4/5 seasons. The apparent "more park, less opponent" tilt
#   is carried almost entirely by 2021 and 2025 — 2022/2023/2024 prefer 6:3:1.
#
#   THE FINDING WORTH KEEPING is the diagnostic, not the verdict. Inside the
#   band SP Skillz's spread collapses (SD 10.03 -> 5.94) while Team Rater
#   (9.88 -> 9.16) and Park Factor (9.63 -> 9.14) barely compress. Standalone
#   AUC-success in band: SP Skillz .5457, Park Factor .5089, Team Rater .4931 —
#   Team Rater is slightly ANTI-predictive there. That is what pulls the grid
#   toward more PF and less TR, and it is an artifact of how the band was cut
#   (within-band sp<->tr correlation -0.616), not a real relationship.
#
#   Practical read: the Coin Flip band is genuinely near-random. Ceiling is
#   ~0.56 AUC however you mix these three. Real gains need a new signal.
#
# ── THE SELECTION CAVEAT (read this before believing any number below) ───────
#   The Coin Flip bucket is DEFINED by the 6:3:1 score, so inside it the three
#   components are mechanically anti-correlated: a high-SP-Skillz arm is only
#   in this band because a tough opponent or park is dragging him down. Any
#   re-weighting is therefore partly just undoing the selection rule that built
#   the band. The script reports the within-band correlation matrix and each
#   component's standalone AUC so that mechanism is visible, and it leans on
#   leave-one-season-out CV (never the in-sample grid maximum) for the verdict.
#
# ── PRE-REGISTERED RULES (fixed before results were looked at) ───────────────
#   Sample     2021-2025 point-in-time starts, !spz_placeholder, bucket=="neutral"
#              (identical filter to the shipped calibration panel: n = 10,135)
#   Grid       w_sp, w_tr, w_pf in {0, 0.05, ..., 1}, summing to 1 -> 231 combos
#   Metrics    AUC_success  P(score of a success   > score of a non-success)
#              AUC_bust     P(score of a non-bust  > score of a bust)
#              (both oriented so HIGHER IS BETTER; ties count 0.5)
#              tercile spreads, in percentage points, within the band:
#              succ_spread  top-third success rate  - bottom-third success rate
#              bust_spread  bottom-third bust rate  - top-third bust rate
#   Baseline   6:3:1 (0.60 / 0.30 / 0.10) on the same rows
#   Honesty    leave-one-season-out CV: pick argmax weights on 4 seasons, score
#              the held-out season, compare to 6:3:1 on that same held-out season
#   CIs        1000x bootstrap CLUSTERED ON pitcher_id (484 pitchers, ~21 starts
#              each — start-level resampling would understate the SE badly)
#   ADOPT iff  LOSO mean OOS delta >= +2% relative
#              AND pitcher-clustered bootstrap 95% CI excludes 0
#              AND >= 4/5 seasons improve
#              (same bar the lens ladder used, so verdicts stay comparable)
#
# ── DATA ─────────────────────────────────────────────────────────────────────
#   Reads the point-in-time backtest built by fbb-tools:
#     scripts/build_stream_calibration.R -> stream_calib_starts_<season>.csv
#   Point-in-time matters: season-final SP Skillz is hindsight and inflates the
#   bucket spread (+31.2 vs +27.3 pts on success, per that script's header).
#   Default path assumes the fbb-tools repo is a sibling checkout; override with
#   --calib-dir. Nothing is fetched — this script is pure analysis on cached CSVs.
#
# Usage
#   Rscript scripts/validation_tuning/streamonator_coinflip_reweight.R
#   Rscript scripts/validation_tuning/streamonator_coinflip_reweight.R \
#       --calib-dir ~/Documents/fbb-tools-repo/data/processed/stream_calibration \
#       --boot 1000 --out data/processed/streamonator_weight_analysis
# ══════════════════════════════════════════════════════════════════════════════

# ── CLI ───────────────────────────────────────────────────────────────────────
args <- commandArgs(trailingOnly = TRUE)
arg_val <- function(flag, default = NULL) {
  i <- match(flag, args)
  if (is.na(i) || i == length(args)) return(default)
  args[i + 1L]
}

CALIB_DIR <- path.expand(arg_val("--calib-dir",
  "~/Documents/fbb-tools-repo/data/processed/stream_calibration"))
OUT_DIR   <- arg_val("--out", file.path("data", "processed", "streamonator_weight_analysis"))
SEASONS   <- as.integer(strsplit(arg_val("--seasons", "2021,2022,2023,2024,2025"), ",")[[1]])
B_BOOT    <- as.integer(arg_val("--boot", "1000"))
STEP      <- as.numeric(arg_val("--step", "0.05"))
SEED      <- as.integer(arg_val("--seed", "20260730"))

# Constants mirrored from build_stream_calibration.R — kept here as literals so
# this script runs standalone, but they MUST track that file.
# Held as INTEGERS and divided by their sum, exactly as the builder does. Writing
# it as 0.6/0.3/0.1 instead is not equivalent: those literals aren't exact in
# binary, and 74 of 18,691 starts land the other side of round(x, 1) because of
# it — enough to move starts across the 95/105 band edges.
W_REF_INT <- c(sp = 6, tr = 3, pf = 1)            # live tool 6:3:1
W_REF     <- W_REF_INT / sum(W_REF_INT)
BUCKET_LO <- 95
BUCKET_HI <- 105

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)
set.seed(SEED)

hr <- function(ch = "=") message(strrep(ch, 78))
h1 <- function(txt) { message(""); hr(); message("  ", txt); hr() }

# ── 1. Load the point-in-time backtest ────────────────────────────────────────
h1("1. LOAD — point-in-time starts")

if (!dir.exists(CALIB_DIR))
  stop("Calibration dir not found: ", CALIB_DIR,
       "\n  Build it with fbb-tools scripts/build_stream_calibration.R, or pass --calib-dir.")

starts <- do.call(rbind, lapply(SEASONS, function(y) {
  f <- file.path(CALIB_DIR, sprintf("stream_calib_starts_%d.csv", y))
  if (!file.exists(f)) stop("Missing season file: ", f)
  d <- read.csv(f, stringsAsFactors = FALSE)
  message(sprintf("  %d: %6d rows", y, nrow(d)))
  d
}))
message(sprintf("  loaded %d rows, %d seasons", nrow(starts), length(SEASONS)))

# ── 2. QA: rebuild 6:3:1 and check it against the stored score ────────────────
h1("2. QA — rebuild the shipped 6:3:1 score")

# Same NA fills the builder uses: a missing Team Rater / Park Factor enters at
# neutral 100 rather than dropping the start.
tr_f <- ifelse(is.na(starts$tr_index),    100, starts$tr_index)
pf_f <- ifelse(is.na(starts$park_factor), 100, starts$park_factor)

# Component matrix, already oriented so higher = better for the pitcher.
# TR and PF are inverted (200 - index) exactly as the live tool does.
COMP <- cbind(sp = starts$spz_index, tr = 200 - tr_f, pf = 200 - pf_f)

# Rounded to 1dp because the live module rounds before bucketing — the band
# edges have to fall where the user actually sees them.
score_ref_rounded <- round(as.vector(COMP %*% W_REF_INT) / sum(W_REF_INT), 1)

cmp <- !is.na(starts$streamer_score) & !is.na(score_ref_rounded)
max_dev <- max(abs(score_ref_rounded[cmp] - starts$streamer_score[cmp]))
n_diff  <- sum(score_ref_rounded[cmp] != starts$streamer_score[cmp])
message(sprintf("  rebuilt vs stored streamer_score: %d/%d rows differ, max |dev| = %.4f",
  n_diff, sum(cmp), max_dev))
if (n_diff > 0)
  stop("Rebuilt 6:3:1 score does not match the stored one — component handling has drifted.")
message("  OK — exact match to the shipped builder on every rated start.")

# ── 3. Isolate the Coin Flip band ─────────────────────────────────────────────
h1("3. SAMPLE — isolate the Coin Flip band (95 < 6:3:1 score < 105)")

rated <- !is.na(score_ref_rounded) & !starts$spz_placeholder
bucket <- ifelse(!rated, NA_character_,
          ifelse(score_ref_rounded <= BUCKET_LO, "sit",
          ifelse(score_ref_rounded >= BUCKET_HI, "start", "neutral")))

for (b in c("start", "neutral", "sit")) {
  i <- which(!is.na(bucket) & bucket == b)
  message(sprintf("  %-8s n = %5d   success %.1f%%   bust %.1f%%",
    b, length(i), 100 * mean(starts$success[i]), 100 * mean(starts$bust[i])))
}

flip_i <- which(!is.na(bucket) & bucket == "neutral")
fl <- data.frame(
  season     = starts$season[flip_i],
  pitcher_id = starts$pitcher_id[flip_i],
  success    = as.integer(starts$success[flip_i]),
  bust       = as.integer(starts$bust[flip_i]),
  stringsAsFactors = FALSE
)
FC <- COMP[flip_i, , drop = FALSE]   # flip-band component matrix

message(sprintf("\n  Coin Flip band: n = %d starts, %d pitchers, %d seasons",
  nrow(fl), length(unique(fl$pitcher_id)), length(unique(fl$season))))
message(sprintf("  base rates in band: success %.1f%%   bust %.1f%%",
  100 * mean(fl$success), 100 * mean(fl$bust)))

# ── 4. Why the band behaves the way it does ───────────────────────────────────
h1("4. DIAGNOSTIC — what the components look like INSIDE the band")

message("  Component SDs (index points):")
for (k in colnames(FC))
  message(sprintf("    %-3s  full sample %5.2f   inside band %5.2f",
    k, sd(COMP[rated, k]), sd(FC[, k])))

message("\n  Within-band correlation (Pearson) — negative values are the")
message("  selection rule showing through, not a real-world relationship:")
cr <- cor(FC)
message(sprintf("        %8s %8s %8s", "sp", "tr", "pf"))
for (k in rownames(cr))
  message(sprintf("    %-3s %8.3f %8.3f %8.3f", k, cr[k, 1], cr[k, 2], cr[k, 3]))

# ── 5. Metrics ────────────────────────────────────────────────────────────────
# Mann-Whitney AUC via average ranks (ties -> 0.5), oriented so higher = better.
auc_of <- function(score, pos) {
  n1 <- sum(pos); n0 <- length(pos) - n1
  if (n1 == 0 || n0 == 0) return(NA_real_)
  r <- rank(score)
  (sum(r[pos == 1L]) - n1 * (n1 + 1) / 2) / (n1 * n0)
}

# Decision-relevant spread: sort the band into thirds, compare the ends. This is
# what "which of my coin flips do I actually start?" cashes out to.
tercile_spread <- function(score, outcome) {
  q <- quantile(score, c(1/3, 2/3), na.rm = TRUE, type = 7)
  bot <- score <= q[1]; top <- score > q[2]
  if (!any(bot) || !any(top)) return(NA_real_)
  100 * (mean(outcome[top]) - mean(outcome[bot]))
}

# All four metrics for one weight vector on one subset.
eval_w <- function(w, idx) {
  s <- as.vector(FC[idx, , drop = FALSE] %*% w)
  d <- fl[idx, ]
  c(auc_success = auc_of(s, d$success),
    # non-bust as the positive class => higher score should mean safer
    auc_bust    = auc_of(s, 1L - d$bust),
    succ_spread = tercile_spread(s,  d$success),
    bust_spread = -tercile_spread(s, d$bust))   # sign flipped: higher = better
}

METRICS <- c("auc_success", "auc_bust", "succ_spread", "bust_spread")

# ── 6. Grid ───────────────────────────────────────────────────────────────────
h1(sprintf("6. GRID — %g steps, all combos summing to 1", STEP))

g <- expand.grid(w_sp = seq(0, 1, by = STEP),
                 w_tr = seq(0, 1, by = STEP),
                 w_pf = seq(0, 1, by = STEP))
GRID <- as.matrix(g[abs(rowSums(g) - 1) < 1e-9, , drop = FALSE])
rownames(GRID) <- NULL
message(sprintf("  %d weight combinations", nrow(GRID)))

all_idx <- seq_len(nrow(fl))
grid_res <- t(apply(GRID, 1, function(w) eval_w(w, all_idx)))
grid_df  <- data.frame(GRID, grid_res, stringsAsFactors = FALSE)

ref_row <- eval_w(W_REF, all_idx)
message("\n  Reference 6:3:1 inside the band:")
message(sprintf("    AUC success %.4f | AUC bust %.4f | succ spread %+.1f pp | bust spread %+.1f pp",
  ref_row["auc_success"], ref_row["auc_bust"], ref_row["succ_spread"], ref_row["bust_spread"]))

# Where does 6:3:1 rank among all 231 combos on each metric?
message("\n  6:3:1 rank among all combos (1 = best):")
for (m in METRICS) {
  rk <- sum(grid_df[[m]] > ref_row[m], na.rm = TRUE) + 1L
  message(sprintf("    %-12s rank %3d / %d   (best %.4f, 6:3:1 %.4f, gap %+.4f)",
    m, rk, nrow(grid_df), max(grid_df[[m]], na.rm = TRUE), ref_row[m],
    max(grid_df[[m]], na.rm = TRUE) - ref_row[m]))
}

message("\n  Top 8 combos per metric (in-sample — optimistic by construction):")
for (m in METRICS) {
  message(sprintf("\n    -- %s --", m))
  o <- head(order(-grid_df[[m]]), 8)
  for (i in o)
    message(sprintf("       %.2f / %.2f / %.2f   %s = %8.4f",
      grid_df$w_sp[i], grid_df$w_tr[i], grid_df$w_pf[i], m, grid_df[[m]][i]))
}

# Standalone component AUCs — the single-lens corners of the same grid.
message("\n  Single-component (corner) performance inside the band:")
for (k in seq_len(3)) {
  w <- c(0, 0, 0); w[k] <- 1
  r <- eval_w(w, all_idx)
  message(sprintf("    %-3s only   AUC success %.4f   AUC bust %.4f",
    colnames(FC)[k], r["auc_success"], r["auc_bust"]))
}

# ── 7. Leave-one-season-out CV — the honest number ────────────────────────────
h1("7. LOSO CV — train weights on 4 seasons, score the 5th")

loso_rows <- list()
for (m in METRICS) {
  deltas <- c(); picks <- list()
  for (yr in SEASONS) {
    tr_i <- which(fl$season != yr)
    te_i <- which(fl$season == yr)
    if (!length(te_i)) next
    trn <- apply(GRID, 1, function(w) eval_w(w, tr_i)[m])
    w_best <- GRID[which.max(trn), ]
    got <- eval_w(w_best, te_i)[m]
    base <- eval_w(W_REF, te_i)[m]
    deltas <- c(deltas, got - base)
    picks[[as.character(yr)]] <- w_best
    loso_rows[[length(loso_rows) + 1L]] <- data.frame(
      metric = m, season = yr, w_sp = w_best[1], w_tr = w_best[2], w_pf = w_best[3],
      oos_challenger = got, oos_ref = base, oos_delta = got - base,
      n_test = length(te_i), stringsAsFactors = FALSE)
  }
  # relative improvement is taken against the metric's own scale: AUC is
  # measured above the 0.5 coin-flip floor, spreads against the value itself.
  base_pool <- if (grepl("^auc", m)) ref_row[[m]] - 0.5 else ref_row[[m]]
  rel <- 100 * mean(deltas) / abs(base_pool)
  message(sprintf("\n  %s", m))
  for (nm in names(picks))
    message(sprintf("    hold out %s  ->  trained pick %.2f / %.2f / %.2f",
      nm, picks[[nm]][1], picks[[nm]][2], picks[[nm]][3]))
  message(sprintf("    mean OOS delta %+.4f  (%+.1f%% relative)   seasons improved: %d/%d",
    mean(deltas), rel, sum(deltas > 0), length(deltas)))
}
loso_df <- do.call(rbind, loso_rows)

# ── 7b. Fixed challengers, season by season ───────────────────────────────────
# The LOSO test above re-picks weights inside every fold, so it punishes an
# unstable ARGMAX even when the underlying tilt is real. This section asks the
# blunter shipping question instead: if we had frozen one alternative for all
# five seasons, would it have beaten 6:3:1, and in how many seasons?
#
# These candidates were READ OFF the pooled grid, so they are post-hoc and the
# pooled column is still optimistic. The season-by-season count is the part
# worth reading: a real tilt should show up in most seasons, not just on net.
h1("7b. FIXED CHALLENGERS — frozen weights, season by season")

CANDIDATES <- list(
  "6:2:2   (PF up, TR down)"    = c(0.60, 0.20, 0.20),
  "6.5:2:1.5 (grid argmax AUC)" = c(0.65, 0.20, 0.15),
  "5.5:2.5:2 (grid argmax bust)"= c(0.55, 0.25, 0.20),
  "6:1.5:2.5 (PF heavy)"        = c(0.60, 0.15, 0.25),
  "7:2:1   (skill heavy)"       = c(0.70, 0.20, 0.10)
)

fixed_rows <- list()
for (m in METRICS) {
  message(sprintf("\n  %s   (per-season delta vs 6:3:1; + = challenger better)", m))
  message(sprintf("    %-28s %8s %8s %8s %8s %8s %9s %6s",
    "weights", SEASONS[1], SEASONS[2], SEASONS[3], SEASONS[4], SEASONS[5], "pooled", "n_pos"))
  for (nm in names(CANDIDATES)) {
    w <- CANDIDATES[[nm]]
    per <- vapply(SEASONS, function(yr) {
      i <- which(fl$season == yr)
      eval_w(w, i)[m] - eval_w(W_REF, i)[m]
    }, numeric(1))
    pooled <- eval_w(w, all_idx)[m] - ref_row[[m]]
    message(sprintf("    %-28s %+8.4f %+8.4f %+8.4f %+8.4f %+8.4f %+9.4f %5d/5",
      nm, per[1], per[2], per[3], per[4], per[5], pooled, sum(per > 0)))
    fixed_rows[[length(fixed_rows) + 1L]] <- data.frame(
      metric = m, candidate = nm, w_sp = w[1], w_tr = w[2], w_pf = w[3],
      pooled_delta = pooled, seasons_positive = sum(per > 0),
      t(setNames(per, paste0("d_", SEASONS))), stringsAsFactors = FALSE)
  }
}
fixed_df <- do.call(rbind, fixed_rows)

# ── 8. Pitcher-clustered bootstrap on the pooled challenger ───────────────────
h1(sprintf("8. BOOTSTRAP — %d resamples, clustered on pitcher_id", B_BOOT))

pids     <- unique(fl$pitcher_id)
rows_by  <- split(seq_len(nrow(fl)), fl$pitcher_id)

boot_rows <- list()
for (m in METRICS) {
  w_ch <- GRID[which.max(grid_df[[m]]), ]
  if (isTRUE(all.equal(unname(w_ch), unname(W_REF)))) {
    message(sprintf("\n  %s: in-sample best IS 6:3:1 — nothing to test.", m)); next
  }
  d <- numeric(B_BOOT)
  for (b in seq_len(B_BOOT)) {
    samp <- sample(pids, length(pids), replace = TRUE)
    idx  <- unlist(rows_by[as.character(samp)], use.names = FALSE)
    d[b] <- eval_w(w_ch, idx)[m] - eval_w(W_REF, idx)[m]
  }
  ci <- quantile(d, c(0.025, 0.975), na.rm = TRUE)
  obs <- grid_df[[m]][which.max(grid_df[[m]])] - ref_row[[m]]
  message(sprintf("\n  %s  challenger %.2f / %.2f / %.2f", m, w_ch[1], w_ch[2], w_ch[3]))
  message(sprintf("    in-sample delta %+.4f   boot 95%% CI [%+.4f, %+.4f]   excludes 0: %s",
    obs, ci[1], ci[2], if (ci[1] > 0 || ci[2] < 0) "YES" else "no"))
  message("    (challenger was picked ON this data — CI is optimistic; LOSO above is the verdict)")
  boot_rows[[length(boot_rows) + 1L]] <- data.frame(
    metric = m, w_sp = w_ch[1], w_tr = w_ch[2], w_pf = w_ch[3],
    insample_delta = obs, ci_lo = ci[1], ci_hi = ci[2],
    excludes_zero = (ci[1] > 0 || ci[2] < 0), B = B_BOOT, stringsAsFactors = FALSE)
}
boot_df <- if (length(boot_rows)) do.call(rbind, boot_rows) else NULL

# ── 9. Verdict against the pre-registered bar ─────────────────────────────────
h1("9. VERDICT — pre-registered adoption rule")

message("  Adopt a challenger only if ALL THREE hold:")
message("    (a) LOSO mean OOS delta >= +2% relative")
message("    (b) pitcher-clustered bootstrap 95% CI excludes 0")
message("    (c) >= 4/5 seasons improve out of sample\n")

verdict_rows <- list()
for (m in METRICS) {
  sub <- loso_df[loso_df$metric == m, ]
  if (!nrow(sub)) next
  base_pool <- if (grepl("^auc", m)) ref_row[[m]] - 0.5 else ref_row[[m]]
  rel  <- 100 * mean(sub$oos_delta) / abs(base_pool)
  nimp <- sum(sub$oos_delta > 0)
  bt   <- if (!is.null(boot_df)) boot_df[boot_df$metric == m, ] else NULL
  ci_ok <- if (!is.null(bt) && nrow(bt)) bt$excludes_zero[1] else FALSE
  # weight-stability check: did every LOSO fold land on the same weights?
  stable <- nrow(unique(sub[, c("w_sp","w_tr","w_pf")])) == 1L
  pass <- (rel >= 2) && ci_ok && (nimp >= 4)
  message(sprintf("  %-12s  rel %+6.1f%%  |  CI excl 0: %-3s  |  seasons %d/%d  |  fold picks %s  =>  %s",
    m, rel, if (ci_ok) "yes" else "no", nimp, nrow(sub),
    if (stable) "stable" else "UNSTABLE", if (pass) "ADOPT" else "REJECT"))
  verdict_rows[[length(verdict_rows) + 1L]] <- data.frame(
    metric = m, rel_pct = rel, ci_excludes_zero = ci_ok,
    seasons_improved = nimp, seasons_total = nrow(sub),
    fold_picks_stable = stable, verdict = if (pass) "ADOPT" else "REJECT",
    stringsAsFactors = FALSE)
}
verdict_df <- do.call(rbind, verdict_rows)

# ── 10. Write outputs ─────────────────────────────────────────────────────────
h1("10. OUTPUTS")

f_grid  <- file.path(OUT_DIR, "coinflip_reweight_grid.csv")
f_loso  <- file.path(OUT_DIR, "coinflip_reweight_loso.csv")
f_fixed <- file.path(OUT_DIR, "coinflip_reweight_fixed_candidates.csv")
f_boot  <- file.path(OUT_DIR, "coinflip_reweight_bootstrap.csv")
f_verd  <- file.path(OUT_DIR, "coinflip_reweight_verdict.csv")

write.csv(grid_df,  f_grid,  row.names = FALSE)
write.csv(loso_df,  f_loso,  row.names = FALSE)
write.csv(fixed_df, f_fixed, row.names = FALSE)
if (!is.null(boot_df)) write.csv(boot_df, f_boot, row.names = FALSE)
write.csv(verdict_df, f_verd, row.names = FALSE)

for (f in c(f_grid, f_loso, f_fixed, if (!is.null(boot_df)) f_boot, f_verd))
  message("  wrote ", f)
message("")
