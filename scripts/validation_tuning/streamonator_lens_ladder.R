#!/usr/bin/env Rscript
# streamonator_lens_ladder.R
#
# Phase 1 of the iPF x Streamonator validation (July 2026): the COMPLEXITY LADDER.
# Governing principle (user, 2026-07-02): a simple interpretable solution that is
# ~98% as good beats a complex one — complexity must pay rent.
#
# SCORE LADDER — what feeds the PF slot of the 6:3:1 composite:
#   S-1  no park factor at all            (floor: simplest possible)
#   S0   Overall lens (direct wOBAcon)    (baseline: status quo + fresh data)
#   S1   best single alternative lens     (BACON / HR / Carry — still one column)
#   S2   best two-lens mix (0.1 steps)    (mild complexity)
#   S3   best four-lens mix (0.1 steps)   (full sub-weight machinery)
#
# PRE-REGISTERED ADOPTION RULES (a rung replaces the one below only if ALL hold,
# evaluated leave-one-season-out so selection never sees its test season):
#   (i)   primary caution metric M1q improves by >= 2% RELATIVE
#   (ii)  paired bootstrap 95% CI of the pooled improvement excludes zero
#   (iii) improvement direction holds in >= 4 of 5 seasons
#
# SCOREBOARD (zones held at fixed SHARES from the S0 score distribution so all
# candidates are judged on identical fractions of starts):
#   M1q  ER5+ rate inside the top start-zone share          (primary, lower = better)
#   M2q  share of all ER5+ starts landing in the sit zone   (higher = better)
#   M3q  good-start rate inside the start zone              (higher = better)
#   rho  Spearman vs GSM 0-4                                (continuity metric)
#
# RISK LADDER — leave-one-season-out logistic for P(ER5+), the "Risk column":
#   R1  SPZ + TR              R2  + park Overall         R3  + BACON + HR + BACON:HR
#   Utility metric: OOS blow-up-rate spread between top and bottom risk terciles
#   INSIDE the flip zone (95-105) — the zone where decisions actually happen.
#
# Also: outer meta-weight recheck (does 6:3:1 hold under the caution scoreboard?)
#
# ── VERDICT (run 2026-07-02): EVERY RUNG REJECTED. Keep S0 Overall. ─────────
#   S1 bacon-swap    LOSO 0.5% WORSE, holds in 1/5 seasons. The pooled bacon
#                    edge (rho +0.0012, boot CI 0.0004-0.0020, positive 5/5)
#                    is REAL but does not replicate on the caution metric.
#                    Textbook "98% as good" territory — complexity unpaid.
#   S2 two-lens      +1.1% but 0/5 seasons. Noise.
#   S3 four-lens     -0.3%, 3/5, CI spans 0, fold winners unstable. Noise.
#   Even PF-AT-ALL vs no-PF is marginal on M1q (-2.4%, 4/5, CI crosses 0).
#   Park's seat in the composite rests on rho, not on caution.
#
#   Meta-weight recheck: 6:3:1 ranks 7/231 on rho (plateau-optimal). Caution-
#   tilted metas (e.g. .30/.35/.35) improve M1q by 4.8% but crater rho to 0.235.
#   Not recommended — that trade is far worse than it looks.
#
#   Risk ladder: R2 (spz+tr+overall) OOS AUC 0.5995, flip-zone spread 4.8pp.
#   R3 (+bacon x hr) AUC 0.6001, spread 5.0pp — exactly AT the pre-registered
#   5pp ship line, and the interaction adds only +0.2pp, failing its own 1pp
#   rule. Deferred.
#
#   SHIPPED OUT OF THIS: only the data refresh (the live app was scoring on a
#   stale May-21 park factor file). Kept Overall lens, 6:3:1, and 105/95
#   thresholds. No sub-weight UI was built. HR x BACON stayed an article aside.
#   Follow-on: streamonator_coinflip_reweight.R re-asked the meta-weight
#   question restricted to the 95-105 band and also rejected (2026-07-30).
#
# Usage: Rscript scripts/validation_tuning/streamonator_lens_ladder.R   (fully cached, no API calls)

IN  <- file.path("data", "processed", "streamonator_weight_analysis", "starts_with_pf_lenses.csv")
OUT <- file.path("data", "processed", "streamonator_weight_analysis", "streamonator_lens_ladder_results.csv")
SEASONS <- 2021:2025
set.seed(42)

st <- read.csv(IN, stringsAsFactors = FALSE)
st <- st[!st$spz_placeholder & !is.na(st$pf_overall_idx), ]
message(sprintf("Sample: %d starts (non-placeholder, lens-matched)", nrow(st)))

inv <- function(x) 200 - x
composite <- function(pf_idx, w = c(6, 3, 1)) {
  vals <- cbind(st$sp_skillz_index, st$team_rater_inv,
                if (is.null(pf_idx)) rep(NA_real_, nrow(st)) else inv(pf_idx))
  wm <- matrix(w, nrow(vals), 3, byrow = TRUE); ok <- !is.na(vals)
  rowSums(vals * wm * ok, na.rm = TRUE) / rowSums(wm * ok)
}

# Fixed zone SHARES from the S0 (Overall lens) score distribution at 105/95
sc0 <- composite(st$pf_overall_idx)
TOP_SHARE <- mean(sc0 > 105)          # start zone share
BOT_SHARE <- mean(sc0 < 95)           # sit zone share
message(sprintf("Fixed zone shares from S0: start %.1f%% | sit %.1f%%", 100 * TOP_SHARE, 100 * BOT_SHARE))

scoreboard <- function(score, idx = rep(TRUE, nrow(st))) {
  s <- score[idx]; blow <- st$blowup_er5[idx]; good <- st$good_start[idx]; gsm <- st$gsm[idx]
  hi <- s >= quantile(s, 1 - TOP_SHARE); lo <- s <= quantile(s, BOT_SHARE)
  c(M1q = mean(blow[hi]),
    M2q = sum(blow & lo) / sum(blow),
    M3q = mean(good[hi]),
    rho = suppressWarnings(cor(s, gsm, method = "spearman")))
}

# ── Candidate mixes ───────────────────────────────────────────────────────────
lens_mat <- as.matrix(st[, c("pf_overall_idx", "pf_bacon_idx", "pf_hr_idx", "pf_carry_idx")])
mix_idx <- function(w4) as.numeric(lens_mat %*% w4)   # w4 sums to 1

grid_w <- function(step, active) {
  g <- expand.grid(rep(list(seq(0, 1, step)), length(active)))
  g <- g[abs(rowSums(g) - 1) < 1e-9, , drop = FALSE]
  out <- matrix(0, nrow(g), 4); out[, active] <- as.matrix(g)
  out
}
# S2 pool: all two-lens pairs; S3 pool: all four lenses
pairs <- combn(1:4, 2, simplify = FALSE)
W2 <- do.call(rbind, lapply(pairs, function(p) grid_w(0.1, p)))
W2 <- unique(W2)
W3 <- grid_w(0.1, 1:4)
message(sprintf("S2 candidates: %d | S3 candidates: %d", nrow(W2), nrow(W3)))

SINGLE <- list(bacon = c(0,1,0,0), hr = c(0,0,1,0), carry = c(0,0,0,1))
W0 <- c(1, 0, 0, 0)   # S0 = overall only

# ── LOSO evaluation of a rung: select on 4 seasons (by M1q mean rank,
#    tiebreak rho), measure winner on held-out season ─────────────────────────
season_scoreboard <- function(score) {
  t(vapply(SEASONS, function(yr) scoreboard(score, st$season == yr), numeric(4)))
}
loso_rung <- function(W) {                       # W: matrix of candidate weights
  sc_list <- lapply(seq_len(nrow(W)), function(i) composite(mix_idx(W[i, ])))
  per_season <- lapply(sc_list, season_scoreboard)   # each: 5 x 4
  fold_rows <- lapply(seq_along(SEASONS), function(k) {
    train <- setdiff(seq_along(SEASONS), k)
    m1_rank  <- rank(vapply(per_season, function(m) mean(m[train, "M1q"]), numeric(1)))
    rho_rank <- rank(-vapply(per_season, function(m) mean(m[train, "rho"]), numeric(1)))
    win <- order(m1_rank + 0.001 * rho_rank)[1]
    data.frame(test_season = SEASONS[k], winner = win,
               w = I(list(W[win, ])),
               M1q = per_season[[win]][k, "M1q"],
               M2q = per_season[[win]][k, "M2q"],
               rho = per_season[[win]][k, "rho"])
  })
  do.call(rbind, fold_rows)
}

# Baseline per-season values (S0 and S-1)
s0_seas  <- season_scoreboard(sc0)
sn_seas  <- season_scoreboard(composite(NULL))

# ── Paired bootstrap of pooled M1q / rho difference vs S0 ────────────────────
boot_vs_s0 <- function(score, B = 1000) {
  n <- nrow(st)
  d <- t(replicate(B, {
    i <- sample.int(n, n, replace = TRUE)
    sb <- score[i]; s0 <- sc0[i]; blow <- st$blowup_er5[i]; gsm <- st$gsm[i]
    hi_b <- sb >= quantile(sb, 1 - TOP_SHARE); hi_0 <- s0 >= quantile(s0, 1 - TOP_SHARE)
    c(dM1 = mean(blow[hi_b]) - mean(blow[hi_0]),
      drho = suppressWarnings(cor(sb, gsm, method = "spearman") -
                              cor(s0, gsm, method = "spearman")))
  }))
  list(dM1_ci = quantile(d[, "dM1"], c(.025, .975)), drho_ci = quantile(d[, "drho"], c(.025, .975)))
}

# ══ SCORE LADDER ══════════════════════════════════════════════════════════════
message("\n══ SCORE LADDER (pooled scoreboard + LOSO verdicts) ══════════════════")
pooled <- rbind(
  `S-1 no PF`   = scoreboard(composite(NULL)),
  `S0 overall`  = scoreboard(sc0),
  `S1 bacon`    = scoreboard(composite(st$pf_bacon_idx)),
  `S1 hr`       = scoreboard(composite(st$pf_hr_idx)),
  `S1 carry`    = scoreboard(composite(st$pf_carry_idx)))
print(round(pooled, 4))

verdicts <- list()
check_rung <- function(label, loso_df, base_seas, boot, base_label) {
  d_m1_seas <- loso_df$M1q - base_seas[, "M1q"]
  rel <- sum(loso_df$M1q * table(st$season)[as.character(SEASONS)]) /
         sum(base_seas[, "M1q"] * table(st$season)[as.character(SEASONS)]) - 1
  n_better <- sum(d_m1_seas < 0)
  pass <- (rel <= -0.02) && (boot$dM1_ci[2] < 0) && (n_better >= 4)
  message(sprintf("  %-22s vs %-10s LOSO M1q rel %+5.1f%% | seasons better %d/5 | boot dM1 CI [%.4f, %.4f] -> %s",
    label, base_label, 100 * rel, n_better, boot$dM1_ci[1], boot$dM1_ci[2],
    ifelse(pass, "ADOPT", "REJECT (stay simple)")))
  data.frame(rung = label, base = base_label, loso_rel_M1q = rel,
             seasons_better = n_better, ci_lo = boot$dM1_ci[1], ci_hi = boot$dM1_ci[2],
             verdict = ifelse(pass, "ADOPT", "REJECT"))
}

message("\n-- Rung S0 vs S-1 (is having a park factor worth it at all?) --")
b <- boot_vs_s0(composite(NULL))   # note: this boots S-1 against S0 (sign flip below)
rel0 <- sum(s0_seas[, "M1q"] * table(st$season)) / sum(sn_seas[, "M1q"] * table(st$season)) - 1
message(sprintf("  S0 vs S-1: pooled M1q rel %+.1f%% | seasons better %d/5 | boot CI (S-1 minus S0) [%.4f, %.4f]",
  100 * rel0, sum(s0_seas[, "M1q"] < sn_seas[, "M1q"]), b$dM1_ci[1], b$dM1_ci[2]))

message("\n-- Rung S1: best single-lens swap (LOSO-selected per fold) --")
W1 <- rbind(bacon = SINGLE$bacon, hr = SINGLE$hr, carry = SINGLE$carry)
l1 <- loso_rung(W1)
message(sprintf("  fold winners: %s", paste(rownames(W1)[l1$winner], collapse = ", ")))
v1 <- check_rung("S1 single-lens swap", l1, s0_seas, boot_vs_s0(composite(st$pf_bacon_idx)), "S0")
verdicts[[1]] <- v1

message("\n-- Rung S2: best two-lens mix --")
l2 <- loso_rung(W2)
w2_str <- vapply(l2$w, function(w) paste(sprintf("%.1f", w), collapse = "/"), character(1))
message(sprintf("  fold winners (o/b/h/c): %s", paste(unique(w2_str), collapse = " | ")))
w2_mode <- l2$w[[which.max(table(w2_str) [w2_str])]]
v2 <- check_rung("S2 two-lens mix", l2, s0_seas, boot_vs_s0(composite(mix_idx(w2_mode))), "S0")
verdicts[[2]] <- v2

message("\n-- Rung S3: best four-lens mix --")
l3 <- loso_rung(W3)
w3_str <- vapply(l3$w, function(w) paste(sprintf("%.1f", w), collapse = "/"), character(1))
message(sprintf("  fold winners (o/b/h/c): %s", paste(unique(w3_str), collapse = " | ")))
w3_mode <- l3$w[[which.max(table(w3_str)[w3_str])]]
v3 <- check_rung("S3 four-lens mix", l3, s0_seas, boot_vs_s0(composite(mix_idx(w3_mode))), "S0")
verdicts[[3]] <- v3

# ══ META-WEIGHT RECHECK ═══════════════════════════════════════════════════════
message("\n══ META-WEIGHT RECHECK (0.05-step outer grid, PF slot = Overall) ═════")
gm <- expand.grid(w_sp = seq(0, 1, .05), w_tr = seq(0, 1, .05), w_pf = seq(0, 1, .05))
gm <- gm[abs(rowSums(gm) - 1) < 1e-9, ]
mres <- do.call(rbind, lapply(seq_len(nrow(gm)), function(i) {
  s <- composite(st$pf_overall_idx, w = as.numeric(gm[i, ]))
  sb <- scoreboard(s)
  data.frame(gm[i, ], M1q = sb["M1q"], rho = sb["rho"])
}))
ref <- mres[abs(mres$w_sp - .6) < .001 & abs(mres$w_tr - .3) < .001 & abs(mres$w_pf - .1) < .001, ]
message(sprintf("  6:3:1 -> rho %.4f (rank %d/%d) | M1q %.4f (rank %d/%d)",
  ref$rho, sum(mres$rho > ref$rho) + 1, nrow(mres),
  ref$M1q, sum(mres$M1q < ref$M1q) + 1, nrow(mres)))
message("  Top 5 by rho:")
top_r <- mres[order(-mres$rho), ][1:5, ]
for (i in 1:5) message(sprintf("    %.2f/%.2f/%.2f  rho %.4f  M1q %.4f",
  top_r$w_sp[i], top_r$w_tr[i], top_r$w_pf[i], top_r$rho[i], top_r$M1q[i]))
message("  Top 5 by M1q (caution):")
top_m <- mres[order(mres$M1q), ][1:5, ]
for (i in 1:5) message(sprintf("    %.2f/%.2f/%.2f  rho %.4f  M1q %.4f",
  top_m$w_sp[i], top_m$w_tr[i], top_m$w_pf[i], top_m$rho[i], top_m$M1q[i]))

# ══ RISK LADDER (LOSO logistic for P(ER5+)) ═══════════════════════════════════
message("\n══ RISK LADDER — LOSO P(ER5+) models, flip-zone utility ══════════════")
st$hr_c  <- (st$pf_hr_idx - 100) / 10
st$bac_c <- (st$pf_bacon_idx - 100) / 10
st$ovr_c <- (st$pf_overall_idx - 100) / 10
forms <- list(
  R1_no_park   = blowup_er5 ~ sp_skillz_index + team_rater_raw,
  R2_overall   = blowup_er5 ~ sp_skillz_index + team_rater_raw + ovr_c,
  R3_hrxbacon  = blowup_er5 ~ sp_skillz_index + team_rater_raw + bac_c * hr_c
)
auc <- function(p, y) {
  r <- rank(p); n1 <- sum(y); n0 <- sum(!y)
  (sum(r[y]) - n1 * (n1 + 1) / 2) / (n1 * n0)
}
flip <- sc0 >= 95 & sc0 <= 105
risk_rows <- list()
for (nm in names(forms)) {
  oos <- rep(NA_real_, nrow(st))
  for (yr in SEASONS) {
    tr_i <- st$season != yr
    m <- glm(forms[[nm]], st[tr_i, ], family = binomial)
    oos[!tr_i] <- predict(m, st[!tr_i, ], type = "response")
  }
  a <- auc(oos, st$blowup_er5)
  ter <- cut(oos[flip], quantile(oos[flip], c(0, 1/3, 2/3, 1)), include.lowest = TRUE,
             labels = c("low", "mid", "high"))
  br <- tapply(st$blowup_er5[flip], ter, mean)
  spread <- br[["high"]] - br[["low"]]
  message(sprintf("  %-12s OOS AUC %.4f | flip-zone blow-up by risk tercile: low %.1f%% mid %.1f%% high %.1f%% (spread %+.1fpp)",
    nm, a, 100 * br[["low"]], 100 * br[["mid"]], 100 * br[["high"]], 100 * spread))
  risk_rows[[nm]] <- data.frame(model = nm, oos_auc = a, flip_spread = spread)
}
message("  Adoption rule: Risk column ships iff OOS flip-zone spread >= 5pp;")
message("  interaction term kept iff it adds >= 1pp spread over R2.")

res <- do.call(rbind, verdicts)
write.csv(res, OUT, row.names = FALSE)
message(sprintf("\nWrote %s", OUT))
