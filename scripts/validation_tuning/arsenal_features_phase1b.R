#!/usr/bin/env Rscript
# ===========================================================================
# Arsenal Features Phase 1b — dose-response, pitch-family variety, joint model
# ===========================================================================
# Follow-ups to arsenal_features_phase1.R (same harnesses, same registered
# screen rules — see that header):
#   1. DOSE-RESPONSE: is the +1-pitch GSM effect linear across 2->3->4->5->6+?
#      (spz-adjusted good rate per n_pitches_10 level, cluster-bootstrap CIs
#      on the marginal jumps)
#   2. FAMILY VARIETY: breaking-ball & offspeed family counts (like the
#      fastball-family finding), specific pairs (slider+sweeper via SLO/ST
#      sub-buckets, curve+knuckle-curve, changeup+splitter), and sub-level
#      total count (sweeper counted separately from slider).
#      Pair testability rule (pre-registered): skip any pair flag with < 30
#      flagged qualified SP-seasons.
#   3. JOINT MODEL: user stance — carry multiple survivors only if the combo
#      beats the best single. Skill side: LOYO ladder base6 -> +n10 -> +fb
#      -> +flag_le2. GSM side: joint logistic coefs w/ cluster bootstrap +
#      AUC progression.
#
# Sub-bucket identities (verified vs Savant arsenal CSV, 593 pitchers 2024):
#   SL = SLO + ST exactly (max dev 0.01pp); CU = CUO + CV exactly.
#   pfx cols are MLBAM/Statcast Gameday classifications (R^2 .94-.98 vs
#   Savant for stable types; SL/ST and CU-family boundaries wobble ~1.5-2pp
#   across classifier vintages — caveat for pair flags).
# ===========================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(glmnet)
  library(jsonlite)
})

set.seed(20260715)

SEASONS   <- 2021:2025
CACHE_DIR <- file.path("data", "raw", "sp_skillz_validation_cache")
STARTS_DIR <- file.path("data", "processed", "streamonator_weight_analysis")
OUT_DIR   <- file.path("data", "processed", "arsenal_research")

SKILL_METRICS <- c("k_minus_bb_pct", "whiff_pct", "stuff_plus", "pitching_plus", "ball_pct", "high_gb_flag")
TARGET_BLEND  <- c(next_k_pct = 1/3, next_whip = 1/3, next_siera = 2/9, next_era = 1/9)
TARGET_SIGN   <- c(next_k_pct = 1, next_whip = -1, next_siera = -1, next_era = -1)
MIN_TBF_FULL <- 100; MIN_IP_NEXT_YEAR <- 40; START_SHARE_MIN <- 2/3
N_BOOT_RESID <- 1000
N_BOOT_GSM   <- 500
MIN_PAIR_N   <- 30

PARENTS <- c("FA","SI","FC","SL","CH","CU","KC","FS","FO","KN","SC","EP")
SUBS    <- c("SLO","ST","CUO","CV")
# sub-level pitch list: SL -> {SLO, ST}; CU -> {CUO, CV}
SUBLEVEL <- c("FA","SI","FC","SLO","ST","CUO","CV","KC","CH","FS","FO","KN","SC","EP")
FB_SET   <- c("FA","SI","FC")
BRK_SET  <- c("SLO","ST","CUO","CV","KC")
OFF_SET  <- c("CH","FS","FO","SC")

cat(strrep("=", 70), "\n")
cat("ARSENAL FEATURES PHASE 1b — dose-response / families / joint model\n")
cat(strrep("=", 70), "\n\n")

# ---------------------------------------------------------------------------
# LOAD (v2 loader + parents + sub-buckets + per-pitch Stuff+)
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
    ip = ipb(get_col("IP")), tbf = nc(get_col("TBF")), pitches = nc(get_col("Pitches")),
    start_ip = ipb(get_col("Start-IP")), relief_ip = ipb(get_col("Relief-IP")),
    era = nc(get_col("ERA")), whip = nc(get_col("WHIP")),
    k_pct = nc(get_col("K%")), siera = nc(get_col("SIERA")),
    k_minus_bb_pct = nc(get_col("K-BB%")), balls = nc(get_col("Balls")),
    stuff_plus_raw = nc(get_col("sp_stuff")), pitching_plus_raw = nc(get_col("sp_pitching")),
    gb_pct = nc(get_col("GB%")),
    stringsAsFactors = FALSE
  )
  for (p in c(PARENTS, SUBS)) base[[paste0("u_", p)]] <- nc(get_col(paste0("pfx", p, "%")))
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

# sub-bucket identity assertions
uP <- function(p) { v <- fg_full[[paste0("u_", p)]]; v[is.na(v)] <- 0; v }
stopifnot(max(abs(uP("SL") - uP("SLO") - uP("ST"))) < 0.002)
stopifnot(max(abs(uP("CU") - uP("CUO") - uP("CV"))) < 0.002)
cat("Sub-bucket identities hold: SL = SLO+ST, CU = CUO+CV\n")

# ---------------------------------------------------------------------------
# FEATURES
# ---------------------------------------------------------------------------

Upar <- sapply(PARENTS, uP); Upar <- Upar / pmax(rowSums(Upar), 1e-9)
Usub <- sapply(SUBLEVEL, uP); Usub <- Usub / pmax(rowSums(Usub), 1e-9)

feat <- fg_full %>%
  transmute(
    player_id, player_name, season, pitches, tbf, start_share,
    n_pitches_10     = rowSums(Upar >= 0.10),
    n_pitches_10_sub = rowSums(Usub >= 0.10),
    eff_pitch_count  = 1 / rowSums(Upar^2),
    flag_le2_p10     = as.integer(rowSums(Upar >= 0.10) <= 2),
    fb_family_count_10  = rowSums(Usub[, FB_SET, drop = FALSE] >= 0.10),
    brk_family_count_10 = rowSums(Usub[, BRK_SET, drop = FALSE] >= 0.10),
    off_family_count_10 = rowSums(Usub[, OFF_SET, drop = FALSE] >= 0.10),
    brk_family_count_5  = rowSums(Usub[, BRK_SET, drop = FALSE] >= 0.05),
    off_family_count_5  = rowSums(Usub[, OFF_SET, drop = FALSE] >= 0.05),
    pair_sl_st_10 = as.integer(Usub[, "SLO"] >= 0.10 & Usub[, "ST"] >= 0.10),
    pair_sl_st_5  = as.integer(Usub[, "SLO"] >= 0.05 & Usub[, "ST"] >= 0.05),
    pair_cu_kc_10 = as.integer(Upar[, "CU"] >= 0.10 & Upar[, "KC"] >= 0.10),
    pair_cu_kc_5  = as.integer(Upar[, "CU"] >= 0.05 & Upar[, "KC"] >= 0.05),
    pair_ch_fs_10 = as.integer(Usub[, "CH"] >= 0.10 & Usub[, "FS"] >= 0.10),
    pair_ch_fs_5  = as.integer(Usub[, "CH"] >= 0.05 & Usub[, "FS"] >= 0.05),
    pair_fs_fo_5  = as.integer(Usub[, "FS"] >= 0.05 & Usub[, "FO"] >= 0.05)
  ) %>%
  filter(!is.na(pitches), pitches >= 400)

NEW_FEATURES <- c("n_pitches_10_sub", "brk_family_count_10", "off_family_count_10",
                  "brk_family_count_5", "off_family_count_5",
                  "pair_sl_st_10", "pair_sl_st_5", "pair_cu_kc_10", "pair_cu_kc_5",
                  "pair_ch_fs_10", "pair_ch_fs_5")

qa_pool <- feat %>% filter(start_share >= START_SHARE_MIN, tbf >= MIN_TBF_FULL)
cat(sprintf("\nQualified SP-seasons: %d\n", nrow(qa_pool)))
cat("Prevalence (qualified pool):\n")
for (f in c(NEW_FEATURES, "pair_fs_fo_5")) {
  v <- qa_pool[[f]]
  if (grepl("^pair", f)) {
    cat(sprintf("  %-22s flagged: %d SP-seasons\n", f, sum(v == 1, na.rm = TRUE)))
  } else {
    cat(sprintf("  %-22s levels: %s\n", f,
                paste(sprintf("%s:%d", names(table(v)), table(v)), collapse = "  ")))
  }
}

# ---------------------------------------------------------------------------
# HARNESS PIECES (as phase 1)
# ---------------------------------------------------------------------------

sp_pool_all <- fg_full %>%
  filter(!is.na(start_share), start_share >= START_SHARE_MIN, !is.na(tbf), tbf >= MIN_TBF_FULL) %>%
  inner_join(feat %>% select(player_id, season, n_pitches_10:pair_fs_fo_5),
             by = c("player_id", "season"))

base <- sp_pool_all %>% select(player_id, season, all_of(SKILL_METRICS),
                               n_pitches_10:pair_fs_fo_5)
target <- sp_pool_all %>%
  select(player_id, season, k_pct, siera, whip, era, ip) %>%
  rename(next_k_pct = k_pct, next_siera = siera, next_whip = whip, next_era = era, next_ip = ip) %>%
  mutate(season = season - 1L)
pairs <- inner_join(base, target, by = c("player_id", "season")) %>%
  filter(!is.na(next_ip), next_ip >= MIN_IP_NEXT_YEAR)

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

fit_coef <- function(y, x1, x2, idx = NULL) {
  if (!is.null(idx)) { y <- y[idx]; x1 <- x1[idx]; x2 <- x2[idx] }
  X <- cbind(1, x1, x2)
  f <- tryCatch(suppressWarnings(glm.fit(X, y, family = binomial())), error = function(e) NULL)
  if (is.null(f)) return(NA_real_)
  unname(f$coefficients[3])
}

# GSM sample
starts <- bind_rows(lapply(SEASONS, function(yr) {
  df <- read.csv(file.path(STARTS_DIR, sprintf("starts_%d.csv", yr)), stringsAsFactors = FALSE)
  df$season <- yr
  df
}))
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

gsm <- starts %>%
  inner_join(feat %>% select(player_id, season, n_pitches_10:pair_fs_fo_5),
             by = c("pitcher_id" = "player_id", "season"))
gsm$spz_std <- as.numeric(scale(gsm$sp_skillz_index))
gsm$cluster <- paste(gsm$pitcher_id, gsm$season)
cat(sprintf("\nGSM sample: %d starts matched\n", nrow(gsm)))

# ---------------------------------------------------------------------------
# 1) DOSE-RESPONSE: adjusted good rate by count level
# ---------------------------------------------------------------------------

cat("\n", strrep("=", 70), "\n", sep = "")
cat("1) DOSE-RESPONSE (spz-adjusted good rate per level)\n")
cat(strrep("=", 70), "\n")

dose_response <- function(var, lev_fun, lev_names) {
  d <- gsm[complete.cases(gsm[[var]], gsm$spz_std, gsm$good), ]
  lev <- lev_fun(d[[var]])
  L <- factor(lev, levels = lev_names)
  Xd <- model.matrix(~ spz_std + L, data = data.frame(spz_std = d$spz_std, L = L))
  rate_fun <- function(idx) {
    ff <- tryCatch(suppressWarnings(glm.fit(Xd[idx, , drop = FALSE], d$good[idx],
                                            family = binomial())), error = function(e) NULL)
    if (is.null(ff)) return(rep(NA_real_, length(lev_names)))
    b <- ff$coefficients
    sapply(seq_along(lev_names), function(i) {
      eta <- b[1] + ifelse(i == 1, 0, b[i + 1])   # spz at mean (0)
      plogis(eta)
    })
  }
  obs <- rate_fun(seq_len(nrow(d)))
  clus <- split(seq_len(nrow(d)), d$cluster)
  boots <- replicate(N_BOOT_GSM, {
    idx <- unlist(clus[sample(length(clus), replace = TRUE)], use.names = FALSE)
    rate_fun(idx)
  })
  jumps <- diff(obs)
  jboots <- apply(boots, 2, diff)
  cat(sprintf("\n  %s:\n", var))
  for (i in seq_along(lev_names)) {
    cat(sprintf("    %-4s n=%6d  adj good rate %.1f%%\n",
                lev_names[i], sum(L == lev_names[i]), 100 * obs[i]))
  }
  for (i in seq_along(jumps)) {
    ci <- quantile(jboots[i, ], c(.025, .975), na.rm = TRUE)
    cat(sprintf("    %s->%s: %+.1f pp [%+.1f, %+.1f]\n",
                lev_names[i], lev_names[i + 1], 100 * jumps[i], 100 * ci[1], 100 * ci[2]))
  }
  invisible(list(levels = lev_names, rates = obs, n = table(L)))
}

dose_response("n_pitches_10",
              function(v) ifelse(v <= 2, "<=2", ifelse(v >= 6, "6+", as.character(v))),
              c("<=2", "3", "4", "5", "6+"))
dose_response("fb_family_count_10",
              function(v) ifelse(v >= 3, "3", as.character(v)),
              c("0", "1", "2", "3"))
dose_response("brk_family_count_10",
              function(v) ifelse(v >= 3, "3+", as.character(v)),
              c("0", "1", "2", "3+"))
dose_response("off_family_count_10",
              function(v) ifelse(v >= 2, "2+", as.character(v)),
              c("0", "1", "2+"))

# ---------------------------------------------------------------------------
# 2) SCREEN NEW FEATURES (same dual rules as phase 1)
# ---------------------------------------------------------------------------

cat("\n", strrep("=", 70), "\n", sep = "")
cat("2) DUAL SCREEN — family/pair features\n")
cat(strrep("=", 70), "\n")

testable <- NEW_FEATURES[sapply(NEW_FEATURES, function(f) {
  if (!grepl("^pair", f)) return(TRUE)
  sum(qa_pool[[f]] == 1, na.rm = TRUE) >= MIN_PAIR_N
})]
skipped <- setdiff(NEW_FEATURES, testable)
if (length(skipped)) cat("Skipped (pair prevalence < ", MIN_PAIR_N, "): ",
                         paste(skipped, collapse = ", "), "\n\n", sep = "")

screen_rows <- list()
for (f in testable) {
  # skill side
  per_target_delta <- matrix(NA_real_, nrow = 4, ncol = length(TARGET_BLEND),
                             dimnames = list(NULL, names(TARGET_BLEND)))
  pooled_resid <- c(); pooled_feat <- c(); pooled_pid <- c(); pooled_season <- c()
  for (tgt in names(TARGET_BLEND)) {
    dd <- pairs %>% select(player_id, season, all_of(SKILL_METRICS), all_of(f), all_of(tgt)) %>%
      filter(complete.cases(.))
    if (nrow(dd) < 100 || sd(dd[[f]]) == 0) next
    base_cv <- loyo_cv(dd, SKILL_METRICS, tgt)
    feat_cv <- loyo_cv(dd, c(SKILL_METRICS, f), tgt)
    folds <- intersect(names(base_cv$r2)[is.finite(base_cv$r2)],
                       names(feat_cv$r2)[is.finite(feat_cv$r2)])
    per_target_delta[seq_along(folds), tgt] <- (feat_cv$r2 - base_cv$r2)[folds]
    res <- (dd[[tgt]] - base_cv$oos_pred) * TARGET_SIGN[[tgt]] * TARGET_BLEND[[tgt]]
    ok <- is.finite(res)
    pooled_resid <- c(pooled_resid, res[ok]); pooled_feat <- c(pooled_feat, scale(dd[[f]])[ok])
    pooled_pid <- c(pooled_pid, dd$player_id[ok]); pooled_season <- c(pooled_season, dd$season[ok])
  }
  blend_delta <- as.numeric(per_target_delta %*% TARGET_BLEND)
  blend_delta <- blend_delta[is.finite(blend_delta)]
  s1_mean <- mean(blend_delta); s1_pos <- sum(blend_delta > 0); s1_tot <- length(blend_delta)
  slope_of <- function(idx) unname(coef(lm(pooled_resid[idx] ~ pooled_feat[idx]))[2])
  obs_slope <- slope_of(seq_along(pooled_resid))
  clus <- split(seq_along(pooled_resid), pooled_pid)
  boots <- replicate(N_BOOT_RESID, {
    idx <- unlist(clus[sample(length(clus), replace = TRUE)], use.names = FALSE)
    slope_of(idx)
  })
  ci_s <- quantile(boots, c(.025, .975), na.rm = TRUE)
  tr_signs <- sapply(split(seq_along(pooled_resid), pooled_season), function(ii)
    if (length(ii) < 30) NA_real_ else sign(slope_of(ii)))
  tr_signs <- tr_signs[!is.na(tr_signs)]
  s2_consist <- max(sum(tr_signs > 0), sum(tr_signs < 0))
  pass_s1 <- is.finite(s1_mean) && s1_mean > 0 && s1_pos >= 3
  pass_s2 <- (ci_s[1] > 0 | ci_s[2] < 0) && s2_consist >= 3 &&
             sign(obs_slope) == sign(median(tr_signs))
  v_skill <- ifelse(pass_s1 && pass_s2, "PROMISING", ifelse(pass_s1 || pass_s2, "WEAK", "KILL"))

  # GSM side
  okg <- complete.cases(gsm[[f]], gsm$spz_std, gsm$good)
  d <- gsm[okg, ]
  x <- as.numeric(scale(d[[f]]))
  obs_g <- fit_coef(d$good, d$spz_std, x)
  clusg <- split(seq_len(nrow(d)), d$cluster)
  boots_g <- replicate(N_BOOT_GSM, {
    idx <- unlist(clusg[sample(length(clusg), replace = TRUE)], use.names = FALSE)
    fit_coef(d$good, d$spz_std, x, idx)
  })
  ci_g <- quantile(boots_g, c(.025, .975), na.rm = TRUE)
  seas_signs <- sapply(split(seq_len(nrow(d)), d$season), function(ii)
    if (length(ii) < 200) NA_real_ else sign(fit_coef(d$good, d$spz_std, x, ii)))
  seas_signs <- seas_signs[!is.na(seas_signs)]
  consist_g <- max(sum(seas_signs > 0), sum(seas_signs < 0))
  comp <- sapply(c("ip_ok", "k_ok", "er_ok", "whip_ok"), function(cc)
    fit_coef(as.integer(d[[cc]]), d$spz_std, x))
  pass_g1 <- (ci_g[1] > 0 | ci_g[2] < 0)
  pass_g2 <- consist_g >= 4 && sign(obs_g) == sign(median(seas_signs))
  v_gsm <- ifelse(pass_g1 && pass_g2, "PROMISING", ifelse(pass_g1 || pass_g2, "WEAK", "KILL"))

  screen_rows[[f]] <- data.frame(
    feature = f, d_r2_blend = s1_mean, folds_pos = sprintf("%d/%d", s1_pos, s1_tot),
    resid_slope = obs_slope, resid_lo = ci_s[1], resid_hi = ci_s[2],
    gsm_coef = obs_g, gsm_lo = ci_g[1], gsm_hi = ci_g[2],
    gsm_consist = sprintf("%d/%d", consist_g, length(seas_signs)),
    comp_ip = comp["ip_ok"], comp_k = comp["k_ok"], comp_er = comp["er_ok"], comp_whip = comp["whip_ok"],
    verdict_skill = v_skill, verdict_gsm = v_gsm
  )
  cat(sprintf("  %-22s skill dR2 %+0.4f (%s) resid[%+0.4f,%+0.4f] -> %-9s | gsm %+0.4f [%+0.4f,%+0.4f] %s ip%+.2f -> %s\n",
              f, s1_mean, sprintf("%d/%d", s1_pos, s1_tot), ci_s[1], ci_s[2], v_skill,
              obs_g, ci_g[1], ci_g[2], sprintf("%d/%d", consist_g, length(seas_signs)),
              comp["ip_ok"], v_gsm))
}
screen_tbl <- bind_rows(screen_rows)
write_csv(screen_tbl, file.path(OUT_DIR, "phase1b_family_screen.csv"))

# ---------------------------------------------------------------------------
# 3) JOINT MODEL — does the combo beat the best single?
# ---------------------------------------------------------------------------

cat("\n", strrep("=", 70), "\n", sep = "")
cat("3) JOINT MODEL\n")
cat(strrep("=", 70), "\n")

# Skill-side LOYO ladder
cat("\nSkill-side LOYO ladder (blended CV R^2, complete-case common sample):\n")
LADDER <- list(
  base6        = SKILL_METRICS,
  plus_n10     = c(SKILL_METRICS, "n_pitches_10"),
  plus_n10_fb  = c(SKILL_METRICS, "n_pitches_10", "fb_family_count_10"),
  plus_all3    = c(SKILL_METRICS, "n_pitches_10", "fb_family_count_10", "flag_le2_p10"),
  swap_n10_sub = c(SKILL_METRICS, "n_pitches_10_sub")
)
all_cols <- unique(unlist(LADDER))
ladder_r2 <- list()
for (nm in names(LADDER)) {
  blend_fold <- matrix(NA_real_, nrow = 4, ncol = length(TARGET_BLEND),
                       dimnames = list(NULL, names(TARGET_BLEND)))
  for (tgt in names(TARGET_BLEND)) {
    dd <- pairs %>% select(season, all_of(all_cols), all_of(tgt)) %>% filter(complete.cases(.))
    cv <- loyo_cv(dd, LADDER[[nm]], tgt)
    vals <- cv$r2[is.finite(cv$r2)]
    blend_fold[seq_along(vals), tgt] <- vals
  }
  ladder_r2[[nm]] <- as.numeric(blend_fold %*% TARGET_BLEND)
}
b0 <- ladder_r2$base6
for (nm in names(LADDER)) {
  d <- ladder_r2[[nm]] - b0
  cat(sprintf("  %-14s blended CV R^2 %.4f  (vs base6: %+0.4f, folds+ %d/%d)\n",
              nm, mean(ladder_r2[[nm]], na.rm = TRUE), mean(d, na.rm = TRUE),
              sum(d > 0, na.rm = TRUE), sum(is.finite(d))))
}

# GSM-side joint logistic
cat("\nGSM-side joint logistic (good ~ spz + n10 + fb + flag_le2), cluster bootstrap:\n")
dj <- gsm[complete.cases(gsm$n_pitches_10, gsm$fb_family_count_10, gsm$flag_le2_p10,
                         gsm$spz_std, gsm$good), ]
Xj <- cbind(1, dj$spz_std,
            as.numeric(scale(dj$n_pitches_10)),
            as.numeric(scale(dj$fb_family_count_10)),
            as.numeric(scale(dj$flag_le2_p10)))
colnames(Xj) <- c("int", "spz", "n10", "fb", "le2")
joint_fit <- function(idx) {
  ff <- tryCatch(suppressWarnings(glm.fit(Xj[idx, , drop = FALSE], dj$good[idx],
                                          family = binomial())), error = function(e) NULL)
  if (is.null(ff)) return(rep(NA_real_, 3))
  unname(ff$coefficients[3:5])
}
obs_j <- joint_fit(seq_len(nrow(dj)))
clusj <- split(seq_len(nrow(dj)), dj$cluster)
boots_j <- replicate(N_BOOT_GSM, {
  idx <- unlist(clusj[sample(length(clusj), replace = TRUE)], use.names = FALSE)
  joint_fit(idx)
})
for (i in 1:3) {
  ci <- quantile(boots_j[i, ], c(.025, .975), na.rm = TRUE)
  cat(sprintf("  %-4s joint coef %+0.4f [%+0.4f, %+0.4f]%s\n",
              c("n10", "fb", "le2")[i], obs_j[i], ci[1], ci[2],
              ifelse(ci[1] > 0 | ci[2] < 0, "  *", "")))
}

# AUC progression (in-sample, rank-based)
auc_fun <- function(score, y) {
  r <- rank(score); n1 <- sum(y == 1); n0 <- sum(y == 0)
  (sum(r[y == 1]) - n1 * (n1 + 1) / 2) / (n1 * n0)
}
specs <- list(spz_only = 2, plus_n10 = 3, plus_fb = 4, plus_le2 = 5)
cat("\nAUC progression (in-sample):\n")
for (nm in names(specs)) {
  cols <- seq_len(specs[[nm]])
  ff <- suppressWarnings(glm.fit(Xj[, cols, drop = FALSE], dj$good, family = binomial()))
  sc <- Xj[, cols, drop = FALSE] %*% ff$coefficients
  cat(sprintf("  %-10s AUC %.4f\n", nm, auc_fun(as.numeric(sc), dj$good)))
}

cat(sprintf("\nOutputs:\n  %s\n", file.path(OUT_DIR, "phase1b_family_screen.csv")))
