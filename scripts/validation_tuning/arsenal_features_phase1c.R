#!/usr/bin/env Rscript
# ===========================================================================
# Arsenal Features Phase 1c — Savant SoT rebuild + confirmatory ladders
# ===========================================================================
# User decision 2026-07-15: Baseball Savant is Source of Truth for pitch mix
# (FG pfx and Savant diverge ~1.5-2pp at classifier-vintage boundaries).
# Usage source: data/raw/savant_arsenals/savant_arsenals_YYYY.csv
#   (leaderboard/pitch-arsenals, min=250 pitches, usage %; 10 native types
#    ff si fc sl ch cu fs kn st sv; KC folds into cu — verified via Nola).
#
# GROUPED taxonomy (phase 1b: parent-level counting beat sub-level):
#   members = ff, si, fc, SLfam(sl+st+sv), cu, ch, fs, kn
#   ⚠ SUPERSEDED: the sweeper-aware GRANULAR taxonomy (sweeper and slurve
#     counted as distinct pitches) shipped 2026-07-18 and is what runs now.
#     The grouped taxonomy below is the vintage this file was run under.
#
# ── RESULT (2026-07-15) — THE DECISIVE PHASE ──────────────────────────────
#   SoT SWAP WAS A NON-EVENT: moving from FG pfx to Savant moved almost
#   nothing. n10 exact agreement 94.5% (100% within +/-1); le2 flag 99.6%.
#   Good news — it means the phase 1/1b findings were not classifier artifacts.
#
#   SKILL SIDE: ADOPT n10_sav as the 7th SP Skillz metric. All three
#   pre-registered rules passed:
#     blended LOYO CV R^2 0.3494 -> 0.3536 (+0.0042, 3/4 folds)
#     coefficient quality-aligned on 4/4 targets
#     blended weight +0.62, clearing the 0.05 floor (cf. high_gb_flag 0.37)
#   Payoff concentrates in next_whip (+0.0100 CV R^2) with next_k_pct FLAT.
#   Breadth is a WHIP / run-shape signal, not a strikeout signal.
#
#   STREAMONATOR SIDE: REJECT all three adjustment arms (le2-only -2.5 pts,
#   fb-only +1.1/type, and both). LOSO delta-rho ~0.0000-0.0007, CIs span 0,
#   M1q not improved. This is COHERENT rather than disappointing: a 2.5-point
#   shift on ~7% of starts cannot move pooled rho at this scale (park's entire
#   seat is worth about +0.002), AND once n10 lives inside SP Skillz the
#   composite inherits it anyway — a separate knob would double-count.
#   INTEGRATION POINT IS UPSTREAM ONLY.
#
#   COMPOSITION RESOLVED: joint model gives fb +0.052*, off +0.032*,
#   brk -0.012 n.s. So phase 1b's "breaking-ball penalty" was the mirror of
#   fastball count. The real axes are FASTBALL VARIETY and HAVING AN OFFSPEED.
#
#   ARTICLE-READY DOSE-RESPONSE (Savant SoT, spz-adjusted good rate):
#     n10  <=2 50.3% | 3 55.1% | 4 56.1% | 5+ 56.8%
#          cliff 2->3 = +4.7pp [+2.0, +7.5]; higher jumps n.s.
#     FB   1 53.9% | 2 56.0% (+2.1 [+0.6, +3.7]) | 3 57.5% (+1.5 [-0.3, +3.6])
#
#   PLATOON LIMITATION: statcast_search has NO pitcher x pitch-type group_by
#   (verified — only name, name-date, pitch-type, team-pitch-type; a
#   batter_stands param does exist). Weak-side platoon breadth therefore needs
#   a Phase-2 pitch-level cache. Registered prediction, still untested: the
#   weak-side count should SHARPEN the <=2 cliff.
#
#   ⚠ The candidate weights this file reports are a RESEARCH re-derivation
#     (n10 ~0.62), NOT production. Sample and randomness differ. Production
#     weights come from derive_sp_skillz_weights_v2.R.
#
# PRE-REGISTERED ADOPTION RULES (set before running):
#  A) SKILL (SP Skillz v2.x candidate = n_pitches_10_sav as 7th ridge metric,
#     full weight re-derivation mirroring derive_sp_skillz_weights_v2.R):
#     ADOPT iff (i) blended LOYO dCV-R2 (7m - 6m) > 0 with >= 3/4 folds
#     positive, (ii) quality-aligned ridge coef positive in >= 3/4 targets,
#     (iii) |final blended weight| >= 0.05 (ball_pct precedent).
#  B) STREAMONATOR (per-start adjustment arms on the 6:3:1 composite,
#     LOSO: derive adjustment points on 4 seasons, apply to held-out):
#     Arms: A1 = flag_le2 only, A2 = fb_family count only, A3 = both.
#     Points derivation: logistic good ~ score (train) and good ~ score +
#     features (train); points_f = beta_f / beta_score (score-point units).
#     ADOPT an arm iff pooled paired-bootstrap dSpearman-rho CI (cluster =
#     pitcher-season, 500 reps) excludes 0 positive AND >= 4/5 LOSO seasons
#     rho improves AND pooled M1q relative change <= +2% (caution guard;
#     M1q = ER5+ rate inside fixed top start-zone share, lens-ladder def).
#  C) COMPOSITION (descriptive, no adoption): joint fb/brk/off counts —
#     does breaking-ball negativity survive fastball-count control?
#  D) DOSE-RESPONSE under Savant SoT (article-ready numbers).
#  Weak-side platoon challenger DEFERRED to Phase 2 (statcast_search has no
#  pitcher-x-pitchtype group_by; needs the pitch-level cache).
# ===========================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(glmnet)
  library(jsonlite)
})

set.seed(20260716)

SEASONS   <- 2021:2025
CACHE_DIR <- file.path("data", "raw", "sp_skillz_validation_cache")
ARS_DIR   <- file.path("data", "raw", "savant_arsenals")
LENS_FILE <- file.path("data", "processed", "streamonator_weight_analysis", "starts_with_pf_lenses.csv")
OUT_DIR   <- file.path("data", "processed", "arsenal_research")

SKILL_METRICS <- c("k_minus_bb_pct", "whiff_pct", "stuff_plus", "pitching_plus", "ball_pct", "high_gb_flag")
TARGET_BLEND  <- c(next_k_pct = 1/3, next_whip = 1/3, next_siera = 2/9, next_era = 1/9)
TARGET_SIGN   <- c(next_k_pct = 1, next_whip = -1, next_siera = -1, next_era = -1)
MIN_TBF_FULL <- 100; MIN_IP_NEXT_YEAR <- 40; START_SHARE_MIN <- 2/3
N_BOOT <- 500

NATIVE  <- c("ff","si","fc","sl","ch","cu","fs","kn","st","sv")
FB_SET  <- c("ff","si","fc")
BRK_SET <- c("sl","st","sv","cu")   # native members for composition model
OFF_SET <- c("ch","fs")

cat(strrep("=", 70), "\n")
cat("ARSENAL PHASE 1c — Savant SoT + confirmatory ladders\n")
cat(strrep("=", 70), "\n\n")

# ---------------------------------------------------------------------------
# SAVANT ARSENAL FEATURES
# ---------------------------------------------------------------------------

ars <- bind_rows(lapply(SEASONS, function(y) {
  d <- read.csv(file.path(ARS_DIR, sprintf("savant_arsenals_%d.csv", y)), check.names = FALSE)
  names(d)[1] <- "name"
  out <- data.frame(player_id = as.integer(d$pitcher), season = y)
  for (t in NATIVE) {
    v <- suppressWarnings(as.numeric(d[[paste0("n_", t)]])) / 100
    out[[t]] <- ifelse(is.na(v), 0, v)
  }
  out
}))
U <- as.matrix(ars[, NATIVE])
rs <- rowSums(U)
cat(sprintf("Arsenal rows: %d | share sums median %.3f, frac in [.99,1.01]: %.4f\n",
            nrow(ars), median(rs), mean(rs >= .99 & rs <= 1.01)))
U <- U / pmax(rs, 1e-9)

# grouped taxonomy
G <- cbind(ff = U[, "ff"], si = U[, "si"], fc = U[, "fc"],
           slfam = U[, "sl"] + U[, "st"] + U[, "sv"],
           cu = U[, "cu"], ch = U[, "ch"], fs = U[, "fs"], kn = U[, "kn"])

sav_feat <- data.frame(
  player_id = ars$player_id, season = ars$season,
  n10_sav        = rowSums(G >= 0.10),
  n10_sav_native = rowSums(U >= 0.10),
  eff_cnt_sav    = 1 / rowSums(G^2),
  flag_le2_sav   = as.integer(rowSums(G >= 0.10) <= 2),
  fb_cnt_sav     = rowSums(U[, FB_SET, drop = FALSE] >= 0.10),
  brk_cnt_sav    = rowSums(U[, BRK_SET, drop = FALSE] >= 0.10),
  off_cnt_sav    = rowSums(U[, OFF_SET, drop = FALSE] >= 0.10)
)
SAV_FEATURES <- setdiff(names(sav_feat), c("player_id", "season"))

# ---------------------------------------------------------------------------
# FG DATA (metrics + targets; pitch mix no longer sourced here)
# ---------------------------------------------------------------------------

nc <- function(x) suppressWarnings(as.numeric(x))
ipb <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  out <- rep(NA_real_, length(x)); keep <- !is.na(x)
  if (!any(keep)) return(out)
  whole <- floor(x[keep]); frac <- round((x[keep] - whole) * 10)
  out[keep] <- whole + ifelse(frac == 1, 1/3, ifelse(frac == 2, 2/3, x[keep] - whole))
  out
}

fg_full <- bind_rows(lapply(SEASONS, function(s) {
  raw <- fromJSON(file.path(CACHE_DIR, sprintf("fg_full_%d_v2.json", s)), flatten = TRUE)$data
  get_col <- function(candidates) {
    for (c in candidates) if (c %in% names(raw)) return(raw[[c]])
    rep(NA, nrow(raw))
  }
  data.frame(
    player_id = as.integer(get_col(c("xMLBAMID", "MLBAMID", "playerid"))),
    season = s,
    ip = ipb(get_col("IP")), tbf = nc(get_col("TBF")), pitches = nc(get_col("Pitches")),
    start_ip = ipb(get_col("Start-IP")), relief_ip = ipb(get_col("Relief-IP")),
    era = nc(get_col("ERA")), whip = nc(get_col("WHIP")),
    k_pct = nc(get_col("K%")), siera = nc(get_col("SIERA")),
    k_minus_bb_pct = nc(get_col("K-BB%")), balls = nc(get_col("Balls")),
    stuff_plus_raw = nc(get_col("sp_stuff")), pitching_plus_raw = nc(get_col("sp_pitching")),
    gb_pct = nc(get_col("GB%")),
    stringsAsFactors = FALSE
  )
}))
savant_whiff <- bind_rows(lapply(SEASONS, function(s) {
  d <- read_csv(file.path(CACHE_DIR, sprintf("savant_whiff_full_%d.csv", s)), show_col_types = FALSE)
  data.frame(player_id = as.integer(d$player_id), season = s,
             whiff_pct = 100 * as.numeric(d$whiffs) / as.numeric(d$swings))
}))
savant_whiff <- savant_whiff[!duplicated(savant_whiff[, c("player_id", "season")]), ]
fg_full <- merge(fg_full, savant_whiff, by = c("player_id", "season"), all.x = TRUE)
fg_full$ball_pct <- ifelse(fg_full$pitches > 0, 100 * fg_full$balls / fg_full$pitches, NA_real_)
fg_full$start_ip[is.na(fg_full$start_ip)] <- 0
fg_full$relief_ip[is.na(fg_full$relief_ip)] <- 0
fg_full$start_share <- ifelse((fg_full$start_ip + fg_full$relief_ip) > 0,
                              fg_full$start_ip / (fg_full$start_ip + fg_full$relief_ip), NA_real_)
fg_full$high_gb_flag <- as.integer(!is.na(fg_full$gb_pct) & fg_full$gb_pct >= 0.55)
fg_full$stuff_plus    <- (fg_full$stuff_plus_raw - 100) / 10
fg_full$pitching_plus <- (fg_full$pitching_plus_raw - 100) / 10

pool <- fg_full %>%
  filter(!is.na(start_share), start_share >= START_SHARE_MIN,
         !is.na(tbf), tbf >= MIN_TBF_FULL, !is.na(pitches), pitches >= 400) %>%
  inner_join(sav_feat, by = c("player_id", "season"))
cat(sprintf("Qualified SP-seasons with Savant features: %d\n", nrow(pool)))

# QA: agreement with the FG-based phase-1 features
fg_feat <- read_csv(file.path(OUT_DIR, "arsenal_features_2021_2025.csv"), show_col_types = FALSE) %>%
  select(player_id, season, n_pitches_10, fb_family_count_10, flag_le2_p10)
qa <- pool %>% inner_join(fg_feat, by = c("player_id", "season"))
cat(sprintf("SoT swap agreement (n=%d): n10 exact %.1f%% (|d|<=1: %.1f%%) | fb exact %.1f%% | le2 flag %.1f%%\n\n",
            nrow(qa),
            100 * mean(qa$n10_sav == qa$n_pitches_10),
            100 * mean(abs(qa$n10_sav - qa$n_pitches_10) <= 1),
            100 * mean(qa$fb_cnt_sav == qa$fb_family_count_10),
            100 * mean(qa$flag_le2_sav == qa$flag_le2_p10)))

# ---------------------------------------------------------------------------
# A) SKILL SIDE — full ridge re-derivation, 6 vs 7 metrics
# ---------------------------------------------------------------------------

cat(strrep("=", 70), "\n")
cat("A) SKILL CONFIRMATORY — ridge re-derivation with n10_sav\n")
cat(strrep("=", 70), "\n")

base <- pool %>% select(player_id, season, all_of(SKILL_METRICS), all_of(SAV_FEATURES))
target <- pool %>%
  select(player_id, season, k_pct, siera, whip, era, ip) %>%
  rename(next_k_pct = k_pct, next_siera = siera, next_whip = whip, next_era = era, next_ip = ip) %>%
  mutate(season = season - 1L)
pairs <- inner_join(base, target, by = c("player_id", "season")) %>%
  filter(!is.na(next_ip), next_ip >= MIN_IP_NEXT_YEAR)
cat(sprintf("YoY pairs: %d\n\n", nrow(pairs)))

loyo_cv <- function(df, metric_cols, target_col) {
  seasons <- sort(unique(df$season))
  r2 <- setNames(rep(NA_real_, length(seasons)), seasons)
  for (s in seasons) {
    tr <- df$season != s; te <- !tr
    if (sum(tr) < 20 || sum(te) < 10) next
    Xtr <- as.matrix(df[tr, metric_cols, drop = FALSE]); ytr <- df[[target_col]][tr]
    Xte <- as.matrix(df[te, metric_cols, drop = FALSE]); yte <- df[[target_col]][te]
    mu <- colMeans(Xtr); sd_ <- apply(Xtr, 2, sd); sd_[sd_ == 0] <- 1
    fit <- tryCatch(cv.glmnet(scale(Xtr, mu, sd_), ytr, alpha = 0, nfolds = 5),
                    error = function(e) NULL)
    if (is.null(fit)) next
    pr <- as.numeric(predict(fit, scale(Xte, mu, sd_), s = "lambda.min"))
    r2[as.character(s)] <- 1 - sum((yte - pr)^2) / sum((yte - mean(yte))^2)
  }
  r2
}
full_ridge_coefs <- function(df, metric_cols, target_col) {
  X <- as.matrix(df[, metric_cols, drop = FALSE]); y <- df[[target_col]]
  mu <- colMeans(X); sd_ <- apply(X, 2, sd); sd_[sd_ == 0] <- 1
  fit <- cv.glmnet(scale(X, mu, sd_), y, alpha = 0, nfolds = 10)
  co <- as.numeric(coef(fit, s = "lambda.min"))[-1]
  names(co) <- metric_cols
  co
}

M6 <- SKILL_METRICS
M7 <- c(SKILL_METRICS, "n10_sav")
fold_r2 <- list(m6 = matrix(NA_real_, 4, length(TARGET_BLEND), dimnames = list(NULL, names(TARGET_BLEND))),
                m7 = matrix(NA_real_, 4, length(TARGET_BLEND), dimnames = list(NULL, names(TARGET_BLEND))))
coefs7 <- list(); aligned_pos <- 0
for (tgt in names(TARGET_BLEND)) {
  dd <- pairs %>% select(season, all_of(M7), all_of(tgt)) %>% filter(complete.cases(.))
  r6 <- loyo_cv(dd, M6, tgt); r7 <- loyo_cv(dd, M7, tgt)
  fold_r2$m6[seq_along(r6[is.finite(r6)]), tgt] <- r6[is.finite(r6)]
  fold_r2$m7[seq_along(r7[is.finite(r7)]), tgt] <- r7[is.finite(r7)]
  co <- full_ridge_coefs(dd, M7, tgt)
  coefs7[[tgt]] <- co
  al <- co["n10_sav"] * TARGET_SIGN[[tgt]]
  aligned_pos <- aligned_pos + as.integer(al > 0)
  cat(sprintf("  %-11s n=%4d | CV R2 6m %.4f -> 7m %.4f (d %+0.4f) | n10 coef %+0.5f (aligned %s)\n",
              tgt, nrow(dd), mean(r6, na.rm = TRUE), mean(r7, na.rm = TRUE),
              mean(r7, na.rm = TRUE) - mean(r6, na.rm = TRUE), co["n10_sav"],
              ifelse(al > 0, "+", "-")))
}
blend6 <- as.numeric(fold_r2$m6 %*% TARGET_BLEND)
blend7 <- as.numeric(fold_r2$m7 %*% TARGET_BLEND)
d_blend <- blend7 - blend6
cat(sprintf("\n  Blended CV R2: 6m %.4f -> 7m %.4f | d %+0.4f | folds+ %d/%d\n",
            mean(blend6, na.rm = TRUE), mean(blend7, na.rm = TRUE),
            mean(d_blend, na.rm = TRUE), sum(d_blend > 0, na.rm = TRUE), sum(is.finite(d_blend))))

# blended weight vector (v2 convention: sign-align, blend, scale max = 2)
bw <- setNames(numeric(length(M7)), M7)
for (m in M7) {
  w <- 0
  for (tgt in names(TARGET_BLEND)) w <- w + coefs7[[tgt]][[m]] * TARGET_SIGN[[tgt]] * TARGET_BLEND[[tgt]]
  bw[m] <- w
}
bw <- bw / max(abs(bw)) * 2
cat("\n  Candidate v2.x blended weights (scaled max=2):\n")
for (m in M7) cat(sprintf("    %-16s %+0.4f\n", m, bw[m]))

pass_a1 <- mean(d_blend, na.rm = TRUE) > 0 && sum(d_blend > 0, na.rm = TRUE) >= 3
pass_a2 <- aligned_pos >= 3
pass_a3 <- abs(bw["n10_sav"]) >= 0.05
cat(sprintf("\n  SKILL ADOPTION: dR2 rule %s | coef-alignment rule %s (%d/4) | weight rule %s (%.3f) -> %s\n",
            ifelse(pass_a1, "PASS", "FAIL"), ifelse(pass_a2, "PASS", "FAIL"), aligned_pos,
            ifelse(pass_a3, "PASS", "FAIL"), bw["n10_sav"],
            ifelse(pass_a1 && pass_a2 && pass_a3, "ADOPT", "REJECT")))

# ---------------------------------------------------------------------------
# B) STREAMONATOR LADDER (LOSO adjustment arms)
# ---------------------------------------------------------------------------

cat("\n", strrep("=", 70), "\n", sep = "")
cat("B) STREAMONATOR LADDER — arsenal adjustment arms (LOSO)\n")
cat(strrep("=", 70), "\n")

st <- read.csv(LENS_FILE, stringsAsFactors = FALSE)
st <- st[!st$spz_placeholder & !is.na(st$pf_overall_idx), ]
st <- st %>% inner_join(sav_feat, by = c("pitcher_id" = "player_id", "season"))
st$cluster <- paste(st$pitcher_id, st$season)
cat(sprintf("Sample: %d lens-matched starts with Savant features | good rate %.3f\n",
            nrow(st), mean(st$good_start)))

inv <- function(x) 200 - x
vals <- cbind(st$sp_skillz_index, st$team_rater_inv, inv(st$pf_overall_idx))
wm <- matrix(c(6, 3, 1), nrow(vals), 3, byrow = TRUE); okv <- !is.na(vals)
st$score0 <- rowSums(vals * wm * okv, na.rm = TRUE) / rowSums(wm * okv)
TOP_SHARE <- mean(st$score0 > 105); BOT_SHARE <- mean(st$score0 < 95)
cat(sprintf("Fixed zone shares: start %.1f%% | sit %.1f%%\n\n", 100 * TOP_SHARE, 100 * BOT_SHARE))

scoreboard <- function(score, idx = rep(TRUE, nrow(st))) {
  s <- score[idx]; blow <- st$blowup_er5[idx]; gsm <- st$gsm[idx]
  hi <- s >= quantile(s, 1 - TOP_SHARE)
  c(M1q = mean(blow[hi]), rho = suppressWarnings(cor(s, gsm, method = "spearman")))
}

ARMS <- list(A1_le2 = "flag_le2_sav", A2_fb = "fb_cnt_sav", A3_both = c("flag_le2_sav", "fb_cnt_sav"))
derive_points <- function(train_idx, feats) {
  Xs <- cbind(1, st$score0[train_idx])
  fs <- suppressWarnings(glm.fit(Xs, st$good_start[train_idx], family = binomial()))
  beta_score <- unname(fs$coefficients[2])
  Xf <- cbind(1, st$score0[train_idx], as.matrix(st[train_idx, feats, drop = FALSE]))
  ff <- suppressWarnings(glm.fit(Xf, st$good_start[train_idx], family = binomial()))
  setNames(unname(ff$coefficients[-(1:2)]) / beta_score, feats)
}

ladder_rows <- list()
for (arm in names(ARMS)) {
  feats <- ARMS[[arm]]
  adj <- rep(NA_real_, nrow(st)); pts_log <- list()
  for (yr in SEASONS) {
    te <- st$season == yr
    pts <- derive_points(!te, feats)
    pts_log[[as.character(yr)]] <- pts
    adj[te] <- st$score0[te] + as.matrix(st[te, feats, drop = FALSE]) %*% pts
  }
  per_season <- t(vapply(SEASONS, function(yr) {
    idx <- st$season == yr
    scoreboard(adj, idx) - scoreboard(st$score0, idx)
  }, numeric(2)))
  n_by_season <- as.numeric(table(st$season)[as.character(SEASONS)])
  base_m1 <- vapply(SEASONS, function(yr) scoreboard(st$score0, st$season == yr)["M1q"], numeric(1))
  adj_m1  <- vapply(SEASONS, function(yr) scoreboard(adj,       st$season == yr)["M1q"], numeric(1))
  m1_rel <- sum(adj_m1 * n_by_season) / sum(base_m1 * n_by_season) - 1

  clus <- split(seq_len(nrow(st)), st$cluster)
  boots <- replicate(N_BOOT, {
    idx <- unlist(clus[sample(length(clus), replace = TRUE)], use.names = FALSE)
    suppressWarnings(cor(adj[idx], st$gsm[idx], method = "spearman") -
                     cor(st$score0[idx], st$gsm[idx], method = "spearman"))
  })
  ci <- quantile(boots, c(.025, .975), na.rm = TRUE)
  rho_seas_pos <- sum(per_season[, "rho"] > 0)
  mean_pts <- sapply(feats, function(f) mean(vapply(pts_log, function(p) p[[f]], numeric(1))))

  pass_rho <- ci[1] > 0 && rho_seas_pos >= 4
  pass_m1  <- m1_rel <= 0.02
  verdict <- ifelse(pass_rho && pass_m1, "ADOPT", "REJECT")
  ladder_rows[[arm]] <- data.frame(
    arm = arm, points = paste(sprintf("%s=%+.2f", feats, mean_pts), collapse = " "),
    d_rho_pooled = mean(boots, na.rm = TRUE), rho_lo = ci[1], rho_hi = ci[2],
    rho_seasons_pos = rho_seas_pos, m1q_rel = m1_rel, verdict = verdict
  )
  cat(sprintf("  %-8s pts[%s] | LOSO drho %+.4f [%+.4f, %+.4f] seasons+ %d/5 | M1q rel %+.1f%% -> %s\n",
              arm, ladder_rows[[arm]]$points, mean(boots, na.rm = TRUE), ci[1], ci[2],
              rho_seas_pos, 100 * m1_rel, verdict))
}
ladder_tbl <- bind_rows(ladder_rows)
write_csv(ladder_tbl, file.path(OUT_DIR, "phase1c_streamonator_ladder.csv"))

# ---------------------------------------------------------------------------
# C) COMPOSITION JOINT MODEL (Savant SoT)
# ---------------------------------------------------------------------------

cat("\n", strrep("=", 70), "\n", sep = "")
cat("C) COMPOSITION — joint fb/brk/off counts (good ~ spz + all three)\n")
cat(strrep("=", 70), "\n")

st$spz_std <- as.numeric(scale(st$sp_skillz_index))
dz <- st[complete.cases(st$fb_cnt_sav, st$brk_cnt_sav, st$off_cnt_sav, st$spz_std, st$good_start), ]
Xc <- cbind(1, dz$spz_std, scale(dz$fb_cnt_sav), scale(dz$brk_cnt_sav), scale(dz$off_cnt_sav))
comp_fit <- function(idx) {
  ff <- tryCatch(suppressWarnings(glm.fit(Xc[idx, , drop = FALSE], dz$good_start[idx],
                                          family = binomial())), error = function(e) NULL)
  if (is.null(ff)) return(rep(NA_real_, 3))
  unname(ff$coefficients[3:5])
}
obs_c <- comp_fit(seq_len(nrow(dz)))
clusz <- split(seq_len(nrow(dz)), dz$cluster)
boots_c <- replicate(N_BOOT, {
  idx <- unlist(clusz[sample(length(clusz), replace = TRUE)], use.names = FALSE)
  comp_fit(idx)
})
for (i in 1:3) {
  ci <- quantile(boots_c[i, ], c(.025, .975), na.rm = TRUE)
  cat(sprintf("  %-12s joint coef %+0.4f [%+0.4f, %+0.4f]%s\n",
              c("fb_cnt", "brk_cnt", "off_cnt")[i], obs_c[i], ci[1], ci[2],
              ifelse(ci[1] > 0 | ci[2] < 0, "  *", "")))
}

# ---------------------------------------------------------------------------
# D) DOSE-RESPONSE UNDER SAVANT SoT (article numbers)
# ---------------------------------------------------------------------------

cat("\n", strrep("=", 70), "\n", sep = "")
cat("D) DOSE-RESPONSE (Savant SoT, spz-adjusted good rate)\n")
cat(strrep("=", 70), "\n")

dose <- function(var, lev_fun) {
  d <- st[complete.cases(st[[var]], st$spz_std, st$good_start), ]
  lev <- lev_fun(d[[var]])
  lev_names <- names(sort(table(lev)))[order(as.numeric(gsub("[^0-9.]", "", names(sort(table(lev))))))]
  lev_names <- unique(lev[order(as.numeric(gsub("[^0-9.]", "", lev)))])
  L <- factor(lev, levels = lev_names)
  Xd <- model.matrix(~ spz + L, data = data.frame(spz = d$spz_std, L = L))
  rate_fun <- function(idx) {
    ff <- tryCatch(suppressWarnings(glm.fit(Xd[idx, , drop = FALSE], d$good_start[idx],
                                            family = binomial())), error = function(e) NULL)
    if (is.null(ff)) return(rep(NA_real_, length(lev_names)))
    b <- ff$coefficients
    vapply(seq_along(lev_names), function(i) plogis(b[1] + ifelse(i == 1, 0, b[i + 1])), numeric(1))
  }
  obs <- rate_fun(seq_len(nrow(d)))
  clus <- split(seq_len(nrow(d)), d$cluster)
  boots <- replicate(N_BOOT, {
    idx <- unlist(clus[sample(length(clus), replace = TRUE)], use.names = FALSE)
    rate_fun(idx)
  })
  cat(sprintf("\n  %s:\n", var))
  for (i in seq_along(lev_names))
    cat(sprintf("    %-4s n=%6d  adj good rate %.1f%%\n", lev_names[i], sum(L == lev_names[i]), 100 * obs[i]))
  jumps <- diff(obs); jb <- apply(boots, 2, diff)
  if (length(lev_names) == 2) jb <- matrix(jb, nrow = 1)
  for (i in seq_along(jumps)) {
    ci <- quantile(jb[i, ], c(.025, .975), na.rm = TRUE)
    cat(sprintf("    %s->%s: %+.1f pp [%+.1f, %+.1f]\n",
                lev_names[i], lev_names[i + 1], 100 * jumps[i], 100 * ci[1], 100 * ci[2]))
  }
}
dose("n10_sav", function(v) ifelse(v <= 2, "2-", ifelse(v >= 5, "5+", as.character(v))))
# Un-grouped (sweeper/slurve counted as their own pitches) — matches the v2.1+
# engine taxonomy; the article's arsenal cliff is re-run on this.
dose("n10_sav_native", function(v) ifelse(v <= 2, "2-", ifelse(v >= 5, "5+", as.character(v))))
dose("fb_cnt_sav", function(v) ifelse(v <= 1, "1-", ifelse(v >= 3, "3", as.character(v))))

write_csv(pool %>% select(player_id, season, all_of(SAV_FEATURES)),
          file.path(OUT_DIR, "arsenal_features_savant_2021_2025.csv"))
cat(sprintf("\nOutputs:\n  %s\n  %s\n",
            file.path(OUT_DIR, "phase1c_streamonator_ladder.csv"),
            file.path(OUT_DIR, "arsenal_features_savant_2021_2025.csv")))
