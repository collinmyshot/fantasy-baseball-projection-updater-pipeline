#!/usr/bin/env Rscript
# ===========================================================================
# Arsenal Features Phase 1 — screening pitch-mix shape for SP Skillz v2.x
# ===========================================================================
# Question: does arsenal SHAPE (how many pitches, how usage is distributed,
# how per-pitch Stuff+ is distributed) add signal beyond the 6 shipped
# SP Skillz v2 metrics — on either of two targets?
#
#   A) SKILL target: next-season K%/WHIP/SIERA/ERA blend (v2 ridge, LOYO-CV,
#      same pool: start_share >= 2/3, TBF >= 100, next_ip >= 40)
#   B) GSM target: per-start good-start rate (authoritative 0-4 GSM) on the
#      streamonator starts sample, controlling for sp_skillz_index
#
# PRE-REGISTERED SCREEN RULES (set before running, per lens-ladder practice):
#   Skill side — feature is PROMISING iff BOTH:
#     (S1) mean blended dCV-R2 > 0 with >= 3/4 LOYO folds positive
#     (S2) pooled OOS-residual slope bootstrap 95% CI excludes 0
#          AND >= 3/4 season-transitions have the same sign
#   GSM side — feature is PROMISING iff BOTH:
#     (G1) logistic coef (good ~ spz + feature), cluster bootstrap
#          (pitcher-season clusters, 500 reps) 95% CI excludes 0
#     (G2) >= 4/5 seasons same coefficient sign
#   Anything else = KILL for that side. Passing one side and not the other is
#   an allowed, informative outcome (skill vs leash channels differ).
#   Phase 1 is a SCREEN: survivors still need the full ladder (re-derived
#   ridge weights, adoption CIs) before shipping.
#
# Pitch-type vocabulary (verified 2026-07-14 on the cached FG JSONs):
#   pfx PARENT buckets {FA SI FC SL CH CU KC FS FO KN SC EP} sum to 1.0000
#   for every pitcher; ST/SV/SLO are sub-buckets of SL, CUO/CV of CU —
#   parents only here (no double counting). Per-pitch Stuff+ (sp_s_*) uses
#   the same parent vocabulary with FF <-> FA; usage-weighted sp_s_*
#   reproduces overall sp_stuff (checked: Webb 2024, 108.91 vs 108.94).
#
# Caveats (accepted for a screen, flagged for interpretation):
#   - Features are full-season values joined to every start of that season,
#     same convention as sp_skillz_index in the starts sample. Descriptive,
#     not a live walk-forward backtest.
#   - Stuff+ is retrained offseason and retroactively applied (per Eno
#     Sarris), so historical sp_s_* is "today's model on old pitches".
#   - Survivorship: 2-pitch guys still starting are the ones it worked for.
# ===========================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(glmnet)
  library(jsonlite)
})

set.seed(20260714)

SEASONS   <- 2021:2025
CACHE_DIR <- file.path("data", "raw", "sp_skillz_validation_cache")
STARTS_DIR <- file.path("data", "processed", "streamonator_weight_analysis")
OUT_DIR   <- file.path("data", "processed", "arsenal_research")
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

SKILL_METRICS <- c("k_minus_bb_pct", "whiff_pct", "stuff_plus", "pitching_plus", "ball_pct", "high_gb_flag")
TARGET_BLEND  <- c(next_k_pct = 1/3, next_whip = 1/3, next_siera = 2/9, next_era = 1/9)
TARGET_SIGN   <- c(next_k_pct = 1, next_whip = -1, next_siera = -1, next_era = -1)
MIN_TBF_FULL <- 100; MIN_IP_NEXT_YEAR <- 40; START_SHARE_MIN <- 2/3
N_BOOT_RESID <- 1000
N_BOOT_GSM   <- 500

PARENTS <- c("FA","SI","FC","SL","CH","CU","KC","FS","FO","KN","SC","EP")
# sp_s_* uses FF where pfx uses FA
SPS_NAME <- c(FA="sp_s_FF", SI="sp_s_SI", FC="sp_s_FC", SL="sp_s_SL", CH="sp_s_CH",
              CU="sp_s_CU", KC="sp_s_KC", FS="sp_s_FS", FO="sp_s_FO",
              KN=NA, SC=NA, EP=NA)

cat(strrep("=", 70), "\n")
cat("ARSENAL FEATURES PHASE 1 — pre-registered screen\n")
cat(strrep("=", 70), "\n\n")

# ---------------------------------------------------------------------------
# LOAD FG DATA (v2 loader + pfx usage parents + per-pitch Stuff+)
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

cat("Loading FG caches...\n")
fg_full <- bind_rows(lapply(SEASONS, function(s) {
  raw <- fromJSON(file.path(CACHE_DIR, sprintf("fg_full_%d_v2.json", s)), flatten = TRUE)$data
  get_col <- function(candidates) {
    for (c in candidates) if (c %in% names(raw)) return(raw[[c]])
    rep(NA, nrow(raw))
  }
  base <- data.frame(
    player_id = as.integer(get_col(c("xMLBAMID", "MLBAMID", "playerid"))),
    player_name = as.character(get_col(c("PlayerName", "Name"))),
    season = s,
    ip = ipb(get_col("IP")), gs = nc(get_col("GS")),
    tbf = nc(get_col("TBF")), pitches = nc(get_col("Pitches")),
    start_ip = ipb(get_col("Start-IP")), relief_ip = ipb(get_col("Relief-IP")),
    era = nc(get_col("ERA")), whip = nc(get_col("WHIP")),
    k_pct = nc(get_col("K%")), siera = nc(get_col("SIERA")),
    k_minus_bb_pct = nc(get_col("K-BB%")), balls = nc(get_col("Balls")),
    stuff_plus_raw = nc(get_col("sp_stuff")), pitching_plus_raw = nc(get_col("sp_pitching")),
    gb_pct = nc(get_col("GB%")),
    stringsAsFactors = FALSE
  )
  for (p in PARENTS) base[[paste0("u_", p)]] <- nc(get_col(paste0("pfx", p, "%")))
  for (p in PARENTS) {
    sc <- SPS_NAME[[p]]
    base[[paste0("s_", p)]] <- if (!is.na(sc)) nc(get_col(sc)) else NA_real_
  }
  base
}))

savant_full <- bind_rows(lapply(SEASONS, function(s) {
  d <- read_csv(file.path(CACHE_DIR, sprintf("savant_whiff_full_%d.csv", s)), show_col_types = FALSE)
  data.frame(player_id = as.integer(d$player_id), season = s,
             whiff_pct = 100 * as.numeric(d$whiffs) / as.numeric(d$swings))
}))
savant_full <- savant_full[!duplicated(savant_full[, c("player_id", "season")]), ]
fg_full <- merge(fg_full, savant_full, by = c("player_id", "season"), all.x = TRUE)

fg_full$ball_pct <- ifelse(fg_full$pitches > 0, 100 * fg_full$balls / fg_full$pitches, NA_real_)
fg_full$start_ip[is.na(fg_full$start_ip)] <- 0
fg_full$relief_ip[is.na(fg_full$relief_ip)] <- 0
fg_full$start_share <- ifelse((fg_full$start_ip + fg_full$relief_ip) > 0,
                              fg_full$start_ip / (fg_full$start_ip + fg_full$relief_ip), NA_real_)
fg_full$high_gb_flag <- as.integer(!is.na(fg_full$gb_pct) & fg_full$gb_pct >= 0.55)
fg_full$stuff_plus    <- (fg_full$stuff_plus_raw - 100) / 10
fg_full$pitching_plus <- (fg_full$pitching_plus_raw - 100) / 10

# ---------------------------------------------------------------------------
# ARSENAL FEATURES (per pitcher-season; require pitches >= 400 for stability)
# ---------------------------------------------------------------------------

cat("Building arsenal features...\n")
U <- as.matrix(fg_full[, paste0("u_", PARENTS)])
U[is.na(U)] <- 0
usum <- rowSums(U)
U <- U / ifelse(usum > 0, usum, 1)          # renormalize (sums ~1.0000 anyway)
S <- as.matrix(fg_full[, paste0("s_", PARENTS)])  # per-pitch Stuff+, NA where absent

count_at <- function(floor_) rowSums(U >= floor_)
fb_cols  <- match(c("FA","SI","FC"), PARENTS)

second_best <- function(u_floor) {
  apply(cbind(U, S), 1, function(r) {
    u <- r[seq_along(PARENTS)]; s <- r[-seq_along(PARENTS)]
    ok <- u >= u_floor & !is.na(s)
    if (sum(ok) < 2) return(NA_real_)
    sort(s[ok], decreasing = TRUE)[2]
  })
}
wtd_sd <- function(u_floor) {
  apply(cbind(U, S), 1, function(r) {
    u <- r[seq_along(PARENTS)]; s <- r[-seq_along(PARENTS)]
    ok <- u >= u_floor & !is.na(s)
    if (sum(ok) < 2) return(NA_real_)
    w <- u[ok] / sum(u[ok]); m <- sum(w * s[ok])
    sqrt(sum(w * (s[ok] - m)^2))
  })
}
weapons <- function(u_floor, s_floor) {
  rowSums(U >= u_floor & !is.na(S) & S >= s_floor)
}

feat <- fg_full %>%
  transmute(
    player_id, player_name, season, pitches, tbf, start_share,
    stuff_coverage = NA_real_,
    eff_pitch_count   = 1 / rowSums(U^2),
    n_pitches_10      = count_at(0.10),
    n_pitches_5       = count_at(0.05),
    n_pitches_15      = count_at(0.15),
    flag_le2_p10      = as.integer(count_at(0.10) <= 2),
    flag_ge5_p10      = as.integer(count_at(0.10) >= 5),
    fb_family_count_10     = rowSums(U[, fb_cols, drop = FALSE] >= 0.10),
    fb_family_count_10_noFC = rowSums(U[, fb_cols[1:2], drop = FALSE] >= 0.10),
    best_stuff_10     = apply(ifelse(U >= 0.10 & !is.na(S), S, NA), 1,
                              function(r) if (all(is.na(r))) NA_real_ else max(r, na.rm = TRUE)),
    second_best_stuff_10 = second_best(0.10),
    stuff_spread_5    = wtd_sd(0.05),
    weapons_10_100    = weapons(0.10, 100),
    weapons_5_100     = weapons(0.05, 100),
    weapons_10_105    = weapons(0.10, 105)
  )
feat$stuff_coverage <- rowSums(U * !is.na(S))
feat <- feat %>% filter(!is.na(pitches), pitches >= 400)

ARSENAL_FEATURES <- c("eff_pitch_count", "n_pitches_10", "n_pitches_5", "n_pitches_15",
                      "flag_le2_p10", "flag_ge5_p10",
                      "fb_family_count_10", "fb_family_count_10_noFC",
                      "best_stuff_10", "second_best_stuff_10", "stuff_spread_5",
                      "weapons_10_100", "weapons_5_100", "weapons_10_105")

# ---------------------------------------------------------------------------
# QA BLOCK
# ---------------------------------------------------------------------------

cat("\n--- QA ---\n")
cat(sprintf("Feature rows (pitches>=400): %d pitcher-seasons\n", nrow(feat)))
cat(sprintf("Usage sum check (pre-renorm): median %.4f, range [%.4f, %.4f]\n",
            median(usum[fg_full$pitches >= 400], na.rm = TRUE),
            min(usum[fg_full$pitches >= 400], na.rm = TRUE),
            max(usum[fg_full$pitches >= 400], na.rm = TRUE)))
cat(sprintf("Stuff+ coverage (usage share with sp_s_): median %.3f, p10 %.3f\n",
            median(feat$stuff_coverage, na.rm = TRUE),
            quantile(feat$stuff_coverage, 0.10, na.rm = TRUE)))

# coherence: usage-weighted per-pitch stuff vs overall sp_stuff
uw <- rowSums(U * ifelse(is.na(S), 0, S)) / pmax(rowSums(U * !is.na(S)), 1e-9)
chk <- !is.na(fg_full$stuff_plus_raw) & fg_full$pitches >= 400 & rowSums(U * !is.na(S)) > 0.9
fit_chk <- lm(fg_full$stuff_plus_raw[chk] ~ uw[chk])
cat(sprintf("Coherence: usage-wtd sp_s_* vs sp_stuff R^2 = %.4f (n=%d)\n",
            summary(fit_chk)$r.squared, sum(chk)))

qa_pool <- feat %>% filter(start_share >= START_SHARE_MIN, tbf >= MIN_TBF_FULL)
cat(sprintf("\nQualified-SP feature distribution (n=%d SP-seasons):\n", nrow(qa_pool)))
for (f in c("eff_pitch_count", "n_pitches_10", "second_best_stuff_10", "weapons_10_100")) {
  q <- quantile(qa_pool[[f]], c(.1, .5, .9), na.rm = TRUE)
  cat(sprintf("  %-22s p10 %.2f | median %.2f | p90 %.2f | NA %d\n",
              f, q[1], q[2], q[3], sum(is.na(qa_pool[[f]]))))
}
cat(sprintf("  flag_le2_p10 rate: %.3f | flag_ge5_p10 rate: %.3f\n",
            mean(qa_pool$flag_le2_p10, na.rm = TRUE), mean(qa_pool$flag_ge5_p10, na.rm = TRUE)))

# collinearity map vs existing metrics
sp_pool_all <- fg_full %>%
  filter(!is.na(start_share), start_share >= START_SHARE_MIN, !is.na(tbf), tbf >= MIN_TBF_FULL) %>%
  inner_join(feat %>% select(player_id, season, all_of(ARSENAL_FEATURES)),
             by = c("player_id", "season"))
cat("\nCollinearity (R^2 of feature ~ each existing metric, qualified SPs):\n")
core4 <- c("k_minus_bb_pct", "whiff_pct", "stuff_plus", "pitching_plus")
cat(sprintf("  %-24s %s\n", "feature", paste(sprintf("%-14s", core4), collapse = "")))
for (f in ARSENAL_FEATURES) {
  r2s <- sapply(core4, function(m) {
    ok <- complete.cases(sp_pool_all[[f]], sp_pool_all[[m]])
    if (sum(ok) < 50) return(NA_real_)
    cor(sp_pool_all[[f]][ok], sp_pool_all[[m]][ok])^2
  })
  cat(sprintf("  %-24s %s\n", f, paste(sprintf("%-14.3f", r2s), collapse = "")))
}

# ---------------------------------------------------------------------------
# A) SKILL-SIDE SCREEN (YoY ridge, LOYO-CV — mirrors derive_sp_skillz_weights_v2)
# ---------------------------------------------------------------------------

cat("\n", strrep("=", 70), "\n", sep = "")
cat("A) SKILL TARGET (next-season K%/WHIP/SIERA/ERA blend)\n")
cat(strrep("=", 70), "\n")

sp_full <- sp_pool_all  # qualified SPs with features attached

base <- sp_full %>% select(player_id, season, all_of(SKILL_METRICS), all_of(ARSENAL_FEATURES))
target <- sp_full %>%
  select(player_id, season, k_pct, siera, whip, era, ip) %>%
  rename(next_k_pct = k_pct, next_siera = siera, next_whip = whip, next_era = era, next_ip = ip) %>%
  mutate(season = season - 1L)
pairs <- inner_join(base, target, by = c("player_id", "season")) %>%
  filter(!is.na(next_ip), next_ip >= MIN_IP_NEXT_YEAR)
cat(sprintf("YoY pairs: %d across transitions %s\n\n",
            nrow(pairs), paste(sort(unique(pairs$season)), collapse = ", ")))

# LOYO CV R^2 per fold for a metric set + OOS predictions (for residual screen)
loyo_cv <- function(df, metric_cols, target_col) {
  seasons <- sort(unique(df$season))
  r2 <- setNames(rep(NA_real_, length(seasons)), seasons)
  oos_pred <- rep(NA_real_, nrow(df))
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
    oos_pred[te] <- pr
    r2[as.character(s)] <- 1 - sum((yte - pr)^2) / sum((yte - mean(yte))^2)
  }
  list(r2 = r2, oos_pred = oos_pred)
}

skill_rows <- list()
for (f in ARSENAL_FEATURES) {
  per_target_delta <- matrix(NA_real_, nrow = 4, ncol = length(TARGET_BLEND),
                             dimnames = list(NULL, names(TARGET_BLEND)))
  pooled_resid <- c(); pooled_resid_z <- c()
  pooled_feat <- c(); pooled_pid <- c(); pooled_season <- c()
  n_used <- NA_integer_

  for (tgt in names(TARGET_BLEND)) {
    dd <- pairs %>%
      select(player_id, season, all_of(SKILL_METRICS), all_of(f), all_of(tgt)) %>%
      filter(complete.cases(.))
    n_used <- nrow(dd)
    if (nrow(dd) < 100) next
    base_cv <- loyo_cv(dd, SKILL_METRICS, tgt)
    feat_cv <- loyo_cv(dd, c(SKILL_METRICS, f), tgt)
    folds <- intersect(names(base_cv$r2)[is.finite(base_cv$r2)],
                       names(feat_cv$r2)[is.finite(feat_cv$r2)])
    per_target_delta[seq_along(folds), tgt] <- (feat_cv$r2 - base_cv$r2)[folds]

    # residual screen: baseline OOS residual vs feature (quality-aligned sign)
    res <- (dd[[tgt]] - base_cv$oos_pred) * TARGET_SIGN[[tgt]]
    ok <- is.finite(res)
    # registered stack (raw target units x blend weight) + z-normalized sensitivity
    pooled_resid   <- c(pooled_resid, (res * TARGET_BLEND[[tgt]])[ok])
    pooled_resid_z <- c(pooled_resid_z,
                        (as.numeric(scale(res)) * TARGET_BLEND[[tgt]])[ok])
    pooled_feat  <- c(pooled_feat, scale(dd[[f]])[ok])
    pooled_pid   <- c(pooled_pid, dd$player_id[ok])
    pooled_season <- c(pooled_season, dd$season[ok])
  }

  # blended per-fold delta (weight targets by TARGET_BLEND)
  blend_delta_fold <- as.numeric(per_target_delta %*% TARGET_BLEND)
  blend_delta_fold <- blend_delta_fold[is.finite(blend_delta_fold)]
  s1_mean <- mean(blend_delta_fold)
  s1_pos  <- sum(blend_delta_fold > 0)
  s1_tot  <- length(blend_delta_fold)

  # pooled residual slope (per SD of feature) + cluster bootstrap (by pitcher)
  slope_of <- function(rvec, idx) {
    unname(coef(lm(rvec[idx] ~ pooled_feat[idx]))[2])
  }
  obs_slope <- slope_of(pooled_resid, seq_along(pooled_resid))
  clus <- split(seq_along(pooled_resid), pooled_pid)
  boots <- replicate(N_BOOT_RESID, {
    idx <- unlist(clus[sample(length(clus), replace = TRUE)], use.names = FALSE)
    slope_of(pooled_resid, idx)
  })
  ci <- quantile(boots, c(.025, .975), na.rm = TRUE)
  tr_signs <- sapply(split(seq_along(pooled_resid), pooled_season), function(ii) {
    if (length(ii) < 30) return(NA_real_)
    sign(slope_of(pooled_resid, ii))
  })
  tr_signs <- tr_signs[!is.na(tr_signs)]
  s2_consist <- max(sum(tr_signs > 0), sum(tr_signs < 0))

  # sensitivity: per-target z-scored residuals (removes ERA/SIERA scale dominance)
  obs_slope_z <- slope_of(pooled_resid_z, seq_along(pooled_resid_z))
  boots_z <- replicate(N_BOOT_RESID, {
    idx <- unlist(clus[sample(length(clus), replace = TRUE)], use.names = FALSE)
    slope_of(pooled_resid_z, idx)
  })
  ci_z <- quantile(boots_z, c(.025, .975), na.rm = TRUE)

  pass_s1 <- is.finite(s1_mean) && s1_mean > 0 && s1_pos >= 3
  pass_s2 <- (ci[1] > 0 | ci[2] < 0) && s2_consist >= 3 &&
             sign(obs_slope) == sign(median(tr_signs))
  skill_rows[[f]] <- data.frame(
    feature = f, n_pairs = n_used,
    d_r2_blend = s1_mean, folds_pos = sprintf("%d/%d", s1_pos, s1_tot),
    resid_slope = obs_slope, resid_lo = ci[1], resid_hi = ci[2],
    resid_slope_z = obs_slope_z, resid_z_lo = ci_z[1], resid_z_hi = ci_z[2],
    trans_consist = sprintf("%d/%d", s2_consist, length(tr_signs)),
    verdict_skill = ifelse(pass_s1 && pass_s2, "PROMISING",
                           ifelse(pass_s1 || pass_s2, "WEAK", "KILL"))
  )
  cat(sprintf("  %-24s dR2 %+0.4f (%d/%d folds+) | resid %+0.4f [%+0.4f, %+0.4f] %s | z-sens %+0.4f [%+0.4f, %+0.4f] -> %s\n",
              f, s1_mean, s1_pos, s1_tot, obs_slope, ci[1], ci[2],
              sprintf("%d/%d", s2_consist, length(tr_signs)),
              obs_slope_z, ci_z[1], ci_z[2],
              skill_rows[[f]]$verdict_skill))
}
skill_tbl <- bind_rows(skill_rows)

# ---------------------------------------------------------------------------
# B) GSM-SIDE SCREEN (per-start, control sp_skillz_index)
# ---------------------------------------------------------------------------

cat("\n", strrep("=", 70), "\n", sep = "")
cat("B) GSM TARGET (per-start good-start rate, control = sp_skillz_index)\n")
cat(strrep("=", 70), "\n")

starts <- bind_rows(lapply(SEASONS, function(yr) {
  f <- file.path(STARTS_DIR, sprintf("starts_%d.csv", yr))
  df <- read.csv(f, stringsAsFactors = FALSE)
  df$season <- yr
  df
}))

# authoritative GSM 0-4 (recompute; stored good_start_score is stale)
starts$whip <- ifelse(!is.na(starts$ip) & starts$ip > 0, (starts$h + starts$bb) / starts$ip, Inf)
starts$ip_ok <- !is.na(starts$ip) & starts$ip >= 5.0
starts$k_ok  <- !is.na(starts$k) & !is.na(starts$ip) & starts$k >= (floor(starts$ip) - 1)
starts$er_ok <- !is.na(starts$er) & !is.na(starts$ip) & (
  (starts$ip >= 6.0 & starts$er <= 2) |
  (starts$ip >= 5.0 & starts$ip < 6.0 & starts$er <= 3) |
  (starts$ip >= 4.0 & starts$ip < 5.0 & starts$er <= 2) |
  (starts$ip < 4.0 & starts$er <= 1))
starts$whip_ok <- !is.na(starts$whip) & starts$whip <= 1.18
starts$gsm <- as.integer(starts$ip_ok) + as.integer(starts$k_ok) +
              as.integer(starts$er_ok) + as.integer(starts$whip_ok)
starts$good <- as.integer(starts$gsm >= 3)

starts <- starts %>% filter(!spz_placeholder)
cat(sprintf("Starts sample (placeholders excluded): %d  [expect 22,917]\n", nrow(starts)))
cat(sprintf("Good-start rate: %.3f  [expect ~0.545]\n", mean(starts$good)))

gsm <- starts %>%
  inner_join(feat %>% select(player_id, season, all_of(ARSENAL_FEATURES)),
             by = c("pitcher_id" = "player_id", "season"))
cat(sprintf("Matched to arsenal features: %d starts (%.1f%%)\n\n",
            nrow(gsm), 100 * nrow(gsm) / nrow(starts)))

gsm$spz_std <- as.numeric(scale(gsm$sp_skillz_index))
gsm$cluster <- paste(gsm$pitcher_id, gsm$season)

fit_coef <- function(y, x1, x2, idx = NULL) {
  if (!is.null(idx)) { y <- y[idx]; x1 <- x1[idx]; x2 <- x2[idx] }
  X <- cbind(1, x1, x2)
  f <- tryCatch(suppressWarnings(glm.fit(X, y, family = binomial())), error = function(e) NULL)
  if (is.null(f)) return(NA_real_)
  unname(f$coefficients[3])
}

gsm_rows <- list()
for (f in ARSENAL_FEATURES) {
  ok <- complete.cases(gsm[[f]], gsm$spz_std, gsm$good)
  d <- gsm[ok, ]
  x <- as.numeric(scale(d[[f]]))
  obs <- fit_coef(d$good, d$spz_std, x)

  clus <- split(seq_len(nrow(d)), d$cluster)
  boots <- replicate(N_BOOT_GSM, {
    idx <- unlist(clus[sample(length(clus), replace = TRUE)], use.names = FALSE)
    fit_coef(d$good, d$spz_std, x, idx)
  })
  ci <- quantile(boots, c(.025, .975), na.rm = TRUE)

  seas_signs <- sapply(split(seq_len(nrow(d)), d$season), function(ii) {
    if (length(ii) < 200) return(NA_real_)
    sign(fit_coef(d$good, d$spz_std, x, ii))
  })
  seas_signs <- seas_signs[!is.na(seas_signs)]
  consist <- max(sum(seas_signs > 0), sum(seas_signs < 0))

  # channel decomposition: which GSM component does the feature move?
  comp_coefs <- sapply(c("ip_ok", "k_ok", "er_ok", "whip_ok"), function(cc) {
    fit_coef(as.integer(d[[cc]]), d$spz_std, x)
  })

  pass_g1 <- (ci[1] > 0 | ci[2] < 0)
  pass_g2 <- consist >= 4 && sign(obs) == sign(median(seas_signs))
  gsm_rows[[f]] <- data.frame(
    feature = f, n_starts = nrow(d),
    gsm_coef = obs, gsm_lo = ci[1], gsm_hi = ci[2],
    seasons_consist = sprintf("%d/%d", consist, length(seas_signs)),
    comp_ip = comp_coefs["ip_ok"], comp_k = comp_coefs["k_ok"],
    comp_er = comp_coefs["er_ok"], comp_whip = comp_coefs["whip_ok"],
    verdict_gsm = ifelse(pass_g1 && pass_g2, "PROMISING",
                         ifelse(pass_g1 || pass_g2, "WEAK", "KILL"))
  )
  cat(sprintf("  %-24s coef %+0.4f [%+0.4f, %+0.4f] %s | ip %+0.3f k %+0.3f er %+0.3f whip %+0.3f -> %s\n",
              f, obs, ci[1], ci[2], sprintf("%d/%d", consist, length(seas_signs)),
              comp_coefs["ip_ok"], comp_coefs["k_ok"], comp_coefs["er_ok"], comp_coefs["whip_ok"],
              gsm_rows[[f]]$verdict_gsm))
}
gsm_tbl <- bind_rows(gsm_rows)

# plain-language effect sizes (spz held at mean): adjusted good-rate gap
cat("\nAdjusted good-start-rate effects (sp_skillz held at mean):\n")
eff_specs <- list(
  flag_le2_p10    = c(0, 1),      # arsenal of <=2 pitches: no vs yes
  n_pitches_10    = c(3, 4),      # +1 pitch at the 10% floor
  eff_pitch_count = c(3, 4),      # +1 effective pitch
  fb_family_count_10 = c(1, 2)    # 1 vs 2 fastball types at 10%
)
for (f in names(eff_specs)) {
  ok <- complete.cases(gsm[[f]], gsm$spz_std, gsm$good)
  d <- gsm[ok, ]
  m <- glm(d$good ~ d$spz_std + d[[f]], family = binomial())
  b <- coef(m)
  p_at <- function(v) plogis(b[1] + b[3] * v)
  v <- eff_specs[[f]]
  cat(sprintf("  %-22s %s -> %s: good rate %.1f%% -> %.1f%%  (%+.1f pp)\n",
              f, v[1], v[2], 100 * p_at(v[1]), 100 * p_at(v[2]),
              100 * (p_at(v[2]) - p_at(v[1]))))
}

# ---------------------------------------------------------------------------
# VERDICTS + OUTPUT
# ---------------------------------------------------------------------------

out <- full_join(skill_tbl, gsm_tbl, by = "feature")
write_csv(out, file.path(OUT_DIR, "phase1_screen_results.csv"))
write_csv(feat, file.path(OUT_DIR, "arsenal_features_2021_2025.csv"))

cat("\n", strrep("=", 70), "\n", sep = "")
cat("SUMMARY\n")
cat(strrep("=", 70), "\n")
cat(sprintf("  %-24s %-10s %-10s\n", "feature", "skill", "gsm"))
for (i in seq_len(nrow(out))) {
  cat(sprintf("  %-24s %-10s %-10s\n", out$feature[i], out$verdict_skill[i], out$verdict_gsm[i]))
}
cat(sprintf("\nOutputs:\n  %s\n  %s\n",
            file.path(OUT_DIR, "phase1_screen_results.csv"),
            file.path(OUT_DIR, "arsenal_features_2021_2025.csv")))
