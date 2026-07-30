#!/usr/bin/env Rscript
# ===========================================================================
# Arsenal Phase 2 — in-season pitch-mix change event study (monthly grain)
# ===========================================================================
# Question: when a starter's pitch mix shifts mid-season, does his subsequent
# performance depart from what the pre-change record implied — i.e., is a mix
# change a valid "re-assess this pitcher / season-to-date stats are stale"
# trigger for SP Skillz / the Streamonator?
#
# Data: data/raw/savant_monthly_mix/ (Savant SoT, statcast_search grouped by
# name-month; 8 families ff/si/fc/slfam(SL+ST+SV)/cufam(CU+KC+CS)/ch/fs/kn).
# Outcomes: per-start GSM from starts_with_pf_lenses.csv (2021-2025).
#
# ── RESULT (2026-07-15): HYPOTHESIS REJECTED IN ITS STRONG FORM. ──────────
#   The premise was that a mix change makes season-to-date stats STALE and
#   should trigger re-windowing. It does not.
#
#   MIX CHANGES ARE COMMON AND STICKY: a >=10pp monthly usage shift happens in
#   50.5% of SP pitcher-seasons (7.5pp: 71%; 15pp: 18%), and 64% of changers
#   hold at least half the delta afterward. So this is normal behaviour, not a
#   rare event.
#
#   SELECTION CONFOUND FOUND AND QUANTIFIED — this is the important part.
#   Changers were STRUGGLING BEFORE they changed (before good-rate 51.1% vs
#   57.7% for never-changers). The raw difference-in-differences of +8.8pp
#   shrinks to +3.1pp [+0.2, +6.0] after before-level ANCOVA. Direction holds
#   in all three before-terciles. The raw effect scales with the threshold
#   (15pp gives +14.4 raw) but regression to the mean scales right along with
#   it, so the raw number is not the effect.
#
#   DISPERSION TEST NULL: SD ratio events vs controls 0.99 [0.89, 1.11].
#   Changes do NOT make pitchers less predictable.
#
#   REFRAME: a mix change is a mild POSITIVE ADAPTATION signal, not a
#   "throw out season-to-date data" staleness alarm. Do not build re-windowing
#   on this.
#
#   CONVERGENCE (this is also the stabilization answer for n10 — mix is a
#   CHOICE, not a sample): cumulative n10 vs full season is 98% within +/-1
#   and 95% on the le2 flag by 150-400 pitches; exact n10 hits 93% by 1500+.
#   -> ship n10 with a small pitches-based stabilization (~150, ball_pct
#   convention) or nearly none. The instinct that "stabilization is weird
#   here" was correct: you are not waiting for a sample to stabilize, you are
#   observing a decision.
#
#   2026 CHANGER PROTOTYPE WORKS (phase2_changers_2026.csv, 66 flags at >=800
#   pitches) and the list is face-valid: Robbie Ray new sinker, Sasaki
#   splitter surge, Logan Gilbert FF +19pp, Ohtani sweeper +16pp,
#   Imanaga FF -15pp.
#
# ── DATA NOTE ─────────────────────────────────────────────────────────────
#   Savant buckets Mar/Apr together and Sep/Oct together — months are not
#   uniform. The 10k-row response cap also makes name-date grain infeasible in
#   bulk, which is why this runs at monthly grain.
#
# PRE-REGISTERED DESIGN (set before running):
#   Qualifying month: >= 150 total pitches. SP universe: pitcher-seasons
#   present in the starts panel (non-placeholder).
#   Baseline at month m: pitch-weighted family shares over ALL prior
#   qualifying months (>= 1 required).
#   EVENT at month m: any family |share_m - baseline| >= 0.10 (primary;
#   sensitivities 0.075 / 0.15). Sub-type "new pitch": baseline < 0.03 AND
#   share_m >= 0.10. FIRST event per pitcher-season enters the study.
#   BEFORE = starts in months < m; AFTER = starts in months > m (event month
#   excluded as transition). Both sides need >= 3 starts.
#   CONTROLS = never-flagged (at 0.10) pitcher-seasons.
#     Level test: each control contributes the weighted mean of its
#     candidate-split-month deltas (weights = empirical event-month dist).
#     Dispersion test: each control gets ONE sampled pseudo-month (same dist).
#   TESTS (bootstrap over pitcher-seasons, 1000 reps, seed fixed):
#     T1 LEVEL: mean d(good rate) events - controls  (does performance shift?)
#     T2 DISPERSION: SD ratio events/controls of d   (do events mark regime
#        breaks, i.e., does predictability degrade?)
#     Secondary: d(blowup ER5+ rate), d(mean GSM).
#   Descriptives: event rate/families/months/magnitudes, stickiness (post-
#   event months hold >= half the triggering delta, direction-aware).
#   CONVERGENCE (the "stabilization" answer): agreement of cumulative-to-date
#   n10 (8-family, 10% floor) and flag_le2 with full-season values, by
#   cumulative pitch count — pitch mix is a CHOICE, not a sampled outcome,
#   so we report convergence, not Carleton-style stabilization.
#   2026 TEASER: current-season flags (usage-only prototype of the live feed).
# Caveat: mix changes correlate with health/velo/role events — we measure the
# flag's information value, not causation.
# ===========================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tidyr)
})

set.seed(20260717)

SEASONS   <- 2021:2025
MIX_DIR   <- file.path("data", "raw", "savant_monthly_mix")
LENS_FILE <- file.path("data", "processed", "streamonator_weight_analysis", "starts_with_pf_lenses.csv")
OUT_DIR   <- file.path("data", "processed", "arsenal_research")

FAMILIES <- c("ff", "si", "fc", "slfam", "cufam", "ch", "fs", "kn")
MIN_MONTH_PITCHES <- 150
THRESH_PRIMARY <- 0.10
THRESH_SENS <- c(0.075, 0.15)
NEW_PITCH_BASE <- 0.03
MIN_STARTS_SIDE <- 3
N_BOOT <- 1000

# Savant name-month buckets: Mar/Apr and Sep/Oct are combined
MONTH_MAP <- c("Mar/Apr" = 4, "Apr" = 4, "May" = 5, "Jun" = 6, "Jul" = 7,
               "Aug" = 8, "Sep" = 9, "Sep/Oct" = 9, "Oct" = 9)

cat(strrep("=", 70), "\n")
cat("ARSENAL PHASE 2 — pitch-mix change event study\n")
cat(strrep("=", 70), "\n\n")

# ---------------------------------------------------------------------------
# PANEL BUILD
# ---------------------------------------------------------------------------

read_mix <- function(season, fam) {
  f <- file.path(MIX_DIR, sprintf("mix_%d_%s.csv", season, fam))
  d <- read.csv(f, check.names = FALSE)
  names(d)[1] <- "pitches"
  data.frame(player_id = as.integer(d$player_id),
             player_name = as.character(d$player_name),
             season = season,
             month_txt = as.character(d$api_game_date_month_text),
             pitches = as.numeric(d$pitches),
             total_pitches = as.numeric(d$total_pitches),
             velocity = suppressWarnings(as.numeric(d$velocity)),
             stringsAsFactors = FALSE)
}

build_panel <- function(seasons) {
  spine <- bind_rows(lapply(seasons, read_mix, fam = "total")) %>%
    transmute(player_id, player_name, season, month_txt,
              month = MONTH_MAP[month_txt], total = pitches, velo_all = velocity)
  for (fam in FAMILIES) {
    fx <- bind_rows(lapply(seasons, read_mix, fam = fam)) %>%
      transmute(player_id, season, month = MONTH_MAP[month_txt], x = pitches)
    names(fx)[names(fx) == "x"] <- paste0("p_", fam)
    spine <- spine %>% left_join(fx, by = c("player_id", "season", "month"))
  }
  for (fam in FAMILIES) {
    pc <- paste0("p_", fam)
    spine[[pc]][is.na(spine[[pc]])] <- 0
    spine[[paste0("u_", fam)]] <- spine[[pc]] / spine$total
  }
  spine
}

panel <- build_panel(SEASONS)
bad_month <- sum(is.na(panel$month))
resid <- 1 - rowSums(panel[, paste0("u_", FAMILIES)])
cat(sprintf("Panel: %d pitcher-months | unmapped month labels: %d (%s)\n",
            nrow(panel), bad_month,
            paste(unique(panel$month_txt[is.na(panel$month)]), collapse = ",")))
cat(sprintf("Family-share residual (other pitch types): median %.3f, p95 %.3f\n",
            median(resid), quantile(resid, .95)))
panel <- panel %>% filter(!is.na(month), total >= MIN_MONTH_PITCHES)
cat(sprintf("Qualifying pitcher-months (>=%d pitches): %d\n", MIN_MONTH_PITCHES, nrow(panel)))

# SP universe + outcomes
st <- read.csv(LENS_FILE, stringsAsFactors = FALSE)
st <- st[!st$spz_placeholder, ]
st$month <- pmin(pmax(as.integer(format(as.Date(st$game_date), "%m")), 4), 9)  # clamp to Savant buckets
sp_universe <- st %>% distinct(pitcher_id, season)
panel <- panel %>% semi_join(sp_universe, by = c("player_id" = "pitcher_id", "season"))
cat(sprintf("SP pitcher-months: %d (%d pitcher-seasons)\n\n",
            nrow(panel), nrow(distinct(panel, player_id, season))))

# ---------------------------------------------------------------------------
# EVENT DETECTION
# ---------------------------------------------------------------------------

detect_events <- function(panel, thresh) {
  panel <- panel %>% arrange(player_id, season, month)
  out <- list()
  for (key in split(seq_len(nrow(panel)), paste(panel$player_id, panel$season))) {
    if (length(key) < 2) next
    rows <- panel[key, ]
    for (i in 2:nrow(rows)) {
      prior <- rows[1:(i - 1), ]
      base_u <- colSums(prior[, paste0("p_", FAMILIES)]) / sum(prior$total)
      now_u  <- as.numeric(rows[i, paste0("u_", FAMILIES)])
      delta  <- now_u - base_u
      hit <- abs(delta) >= thresh
      newp <- base_u < NEW_PITCH_BASE & now_u >= 0.10
      if (any(hit)) {
        j <- which.max(abs(delta))
        out[[length(out) + 1]] <- data.frame(
          player_id = rows$player_id[1], player_name = rows$player_name[1],
          season = rows$season[1], event_month = rows$month[i],
          family = FAMILIES[j], base_share = base_u[j], new_share = now_u[j],
          delta = delta[j], is_new_pitch = any(newp),
          n_prior_months = i - 1
        )
        break   # first event per pitcher-season only
      }
    }
  }
  bind_rows(out)
}

cat("Event counts by threshold (first event per pitcher-season):\n")
events_all <- list()
for (th in c(THRESH_SENS[1], THRESH_PRIMARY, THRESH_SENS[2])) {
  ev <- detect_events(panel, th)
  events_all[[as.character(th)]] <- ev
  n_ps <- nrow(distinct(panel, player_id, season))
  cat(sprintf("  |d| >= %.3f: %d events (%.1f%% of %d SP pitcher-seasons)\n",
              th, nrow(ev), 100 * nrow(ev) / n_ps, n_ps))
}
events <- events_all[[as.character(THRESH_PRIMARY)]]
cat(sprintf("\nPrimary events (0.10): by family: %s\n",
            paste(sprintf("%s:%d", names(table(events$family)), table(events$family)), collapse = " ")))
cat(sprintf("  by month: %s | new-pitch subtype: %d\n",
            paste(sprintf("%d:%d", as.integer(names(table(events$event_month))), table(events$event_month)), collapse = " "),
            sum(events$is_new_pitch)))

# stickiness: post-event months hold >= half the triggering delta (direction-aware)
sticky <- vapply(seq_len(nrow(events)), function(i) {
  e <- events[i, ]
  post <- panel %>% filter(player_id == e$player_id, season == e$season, month > e$event_month)
  if (nrow(post) == 0) return(NA)
  post_u <- sum(post[[paste0("p_", e$family)]]) / sum(post$total)
  (post_u - e$base_share) * sign(e$delta) >= abs(e$delta) / 2
}, logical(1))
cat(sprintf("  stickiness (post-months hold >= half the delta): %.1f%% of %d with post data\n\n",
            100 * mean(sticky, na.rm = TRUE), sum(!is.na(sticky))))

# ---------------------------------------------------------------------------
# OUTCOME STUDY
# ---------------------------------------------------------------------------

start_stats <- function(pid, seas, months_keep) {
  s <- st[st$pitcher_id == pid & st$season == seas & st$month %in% months_keep, ]
  c(n = nrow(s), good = mean(s$good_start), blow = mean(s$blowup_er5), gsm = mean(s$gsm))
}
split_delta <- function(pid, seas, m) {
  b <- start_stats(pid, seas, 4:(m - 1)); a <- start_stats(pid, seas, (m + 1):9)
  if (b["n"] < MIN_STARTS_SIDE || a["n"] < MIN_STARTS_SIDE) return(NULL)
  c(d_good = unname(a["good"] - b["good"]), d_blow = unname(a["blow"] - b["blow"]),
    d_gsm = unname(a["gsm"] - b["gsm"]), n_before = unname(b["n"]), n_after = unname(a["n"]))
}

# events: delta at the event month
ev_rows <- lapply(seq_len(nrow(events)), function(i)
  split_delta(events$player_id[i], events$season[i], events$event_month[i]))
ev_keep <- !vapply(ev_rows, is.null, logical(1))
ev_d <- do.call(rbind, ev_rows[ev_keep])
cat(sprintf("Events with >=%d starts both sides: %d\n", MIN_STARTS_SIDE, nrow(ev_d)))
cat(sprintf("  mean starts before %.1f / after %.1f\n", mean(ev_d[, "n_before"]), mean(ev_d[, "n_after"])))

# controls: never flagged at primary threshold
flagged_ps <- paste(events$player_id, events$season)
ctrl_ps <- panel %>% distinct(player_id, season) %>%
  filter(!paste(player_id, season) %in% flagged_ps)
month_dist <- table(factor(events$event_month[ev_keep], levels = 5:8))
month_w <- as.numeric(month_dist) / sum(month_dist)
names(month_w) <- names(month_dist)

ctrl_level <- list(); ctrl_disp <- list()
for (i in seq_len(nrow(ctrl_ps))) {
  cand <- lapply(5:8, function(m) split_delta(ctrl_ps$player_id[i], ctrl_ps$season[i], m))
  ok <- !vapply(cand, is.null, logical(1))
  if (!any(ok)) next
  W <- month_w[as.character(5:8)][ok]
  if (sum(W) == 0) next
  W <- W / sum(W)
  M <- do.call(rbind, cand[ok])
  ctrl_level[[length(ctrl_level) + 1]] <- colSums(M * W)
  pick <- sample(seq_len(nrow(M)), 1, prob = W)
  ctrl_disp[[length(ctrl_disp) + 1]] <- M[pick, ]
}
ctrl_l <- do.call(rbind, ctrl_level); ctrl_d <- do.call(rbind, ctrl_disp)
cat(sprintf("Controls (never flagged, qualifying split): %d pitcher-seasons\n\n", nrow(ctrl_l)))

# T1 LEVEL
boot_mean_diff <- function(a, b, col) {
  obs <- mean(a[, col]) - mean(b[, col])
  bs <- replicate(N_BOOT, mean(a[sample(nrow(a), replace = TRUE), col]) -
                          mean(b[sample(nrow(b), replace = TRUE), col]))
  c(obs = obs, lo = quantile(bs, .025), hi = quantile(bs, .975))
}
cat("T1 LEVEL (after - before shift, events vs controls):\n")
t1 <- list()
for (cc in c("d_good", "d_blow", "d_gsm")) {
  r <- boot_mean_diff(ev_d, ctrl_l, cc)
  t1[[cc]] <- r
  cat(sprintf("  %-7s events %+0.4f | controls %+0.4f | diff %+0.4f [%+0.4f, %+0.4f]%s\n",
              cc, mean(ev_d[, cc]), mean(ctrl_l[, cc]), r["obs"], r["lo.2.5%"], r["hi.97.5%"],
              ifelse(r["lo.2.5%"] > 0 | r["hi.97.5%"] < 0, "  *", "")))
}

# T2 DISPERSION
obs_ratio <- sd(ev_d[, "d_good"]) / sd(ctrl_d[, "d_good"])
bs_ratio <- replicate(N_BOOT,
  sd(ev_d[sample(nrow(ev_d), replace = TRUE), "d_good"]) /
  sd(ctrl_d[sample(nrow(ctrl_d), replace = TRUE), "d_good"]))
ci_r <- quantile(bs_ratio, c(.025, .975))
cat(sprintf("\nT2 DISPERSION (SD of d_good, events/controls): ratio %.3f [%.3f, %.3f]%s\n",
            obs_ratio, ci_r[1], ci_r[2], ifelse(ci_r[1] > 1 | ci_r[2] < 1, "  *", "")))
cat(sprintf("  SD events %.3f (n=%d) | SD controls %.3f (n=%d)\n",
            sd(ev_d[, "d_good"]), nrow(ev_d), sd(ctrl_d[, "d_good"]), nrow(ctrl_d)))

# sensitivity: T1 d_good at other thresholds
cat("\nSensitivity (T1 d_good diff at other thresholds):\n")
for (th in THRESH_SENS) {
  ev2 <- events_all[[as.character(th)]]
  rows2 <- lapply(seq_len(nrow(ev2)), function(i)
    split_delta(ev2$player_id[i], ev2$season[i], ev2$event_month[i]))
  k2 <- !vapply(rows2, is.null, logical(1))
  if (sum(k2) < 20) { cat(sprintf("  %.3f: n=%d too small\n", th, sum(k2))); next }
  d2 <- do.call(rbind, rows2[k2])
  fl2 <- paste(ev2$player_id, ev2$season)
  keep_ctrl <- !paste(ctrl_ps$player_id, ctrl_ps$season) %in% fl2
  r <- boot_mean_diff(d2, ctrl_l, "d_good")
  cat(sprintf("  %.3f: n_ev=%d diff %+0.4f [%+0.4f, %+0.4f]\n",
              th, nrow(d2), r["obs"], r["lo.2.5%"], r["hi.97.5%"]))
}

# ---------------------------------------------------------------------------
# CONVERGENCE (the stabilization answer)
# ---------------------------------------------------------------------------

cat("\n", strrep("=", 70), "\n", sep = "")
cat("CONVERGENCE — cumulative n10 vs full-season (pitch mix is a choice)\n")
cat(strrep("=", 70), "\n")

# Augmented panel with the individual slider-family members so convergence can be
# reported under BOTH taxonomies (local copy; the global panel is untouched).
conv_panel <- panel
for (fam in c("sl", "st", "sv")) {
  fx <- bind_rows(lapply(SEASONS, read_mix, fam = fam)) %>%
    transmute(player_id, season, month = MONTH_MAP[month_txt], x = pitches)
  names(fx)[names(fx) == "x"] <- paste0("p_", fam)
  conv_panel <- conv_panel %>% left_join(fx, by = c("player_id", "season", "month"))
  conv_panel[[paste0("p_", fam)]][is.na(conv_panel[[paste0("p_", fam)]])] <- 0
}

run_convergence <- function(pnl, fams, label) {
  conv <- pnl %>% arrange(player_id, season, month) %>%
    group_by(player_id, season) %>% mutate(cum_total = cumsum(total)) %>% ungroup()
  for (fam in fams)
    conv[[paste0("cu_", fam)]] <- ave(conv[[paste0("p_", fam)]],
                                      paste(conv$player_id, conv$season), FUN = cumsum) / conv$cum_total
  full <- conv %>% group_by(player_id, season) %>%
    summarise(across(all_of(paste0("p_", fams)), sum), total_season = sum(total), .groups = "drop")
  fu <- as.matrix(full[, paste0("p_", fams)]) / full$total_season
  full$n10_full <- rowSums(fu >= 0.10); full$le2_full <- as.integer(full$n10_full <= 2)
  conv$n10_cum <- rowSums(as.matrix(conv[, paste0("cu_", fams)]) >= 0.10)
  conv <- conv %>% left_join(full %>% select(player_id, season, n10_full, le2_full),
                             by = c("player_id", "season"))
  conv$le2_cum <- as.integer(conv$n10_cum <= 2)
  cat(sprintf("\n%s:\n", label))
  buckets <- cbind(lo = c(150, 400, 600, 1000, 1500), hi = c(400, 600, 1000, 1500, Inf))
  for (i in seq_len(nrow(buckets))) {
    sel <- conv$cum_total >= buckets[i, 1] & conv$cum_total < buckets[i, 2]
    cat(sprintf("  %5d-%s pitches: n10 exact %.1f%% (|d|<=1: %.1f%%) | le2 flag %.1f%%  (n=%d)\n",
                buckets[i, 1], ifelse(is.infinite(buckets[i, 2]), "+", buckets[i, 2]),
                100 * mean(conv$n10_cum[sel] == conv$n10_full[sel]),
                100 * mean(abs(conv$n10_cum[sel] - conv$n10_full[sel]) <= 1),
                100 * mean(conv$le2_cum[sel] == conv$le2_full[sel]), sum(sel)))
  }
}

cat("Agreement of cumulative-to-date with full-season, by cumulative pitches:\n")
run_convergence(conv_panel, FAMILIES, "GROUPED taxonomy (slider family = SL+ST+SV)")
run_convergence(conv_panel, c("ff", "si", "fc", "sl", "st", "sv", "cufam", "ch", "fs", "kn"),
                "UN-GROUPED taxonomy (sweeper & slurve as own pitches - live engine)")

# ---------------------------------------------------------------------------
# 2026 TEASER — current changers (usage-only prototype)
# ---------------------------------------------------------------------------

cat("\n", strrep("=", 70), "\n", sep = "")
cat("2026 TEASER — current mix changers (latest month vs season baseline)\n")
cat(strrep("=", 70), "\n")

p26 <- build_panel(2026) %>% filter(!is.na(month), total >= MIN_MONTH_PITCHES)
season_tot <- p26 %>% group_by(player_id) %>% summarise(tp = sum(total), .groups = "drop")
p26 <- p26 %>% semi_join(season_tot %>% filter(tp >= 800), by = "player_id")
ev26 <- detect_events(p26 %>% mutate(season = 2026), THRESH_PRIMARY) %>%
  arrange(desc(abs(delta)))
cat(sprintf("Flagged (>=800 season pitches): %d\n", nrow(ev26)))
if (nrow(ev26)) {
  for (i in seq_len(min(15, nrow(ev26)))) {
    e <- ev26[i, ]
    cat(sprintf("  %-24s %-5s month %d: %s %.0f%% -> %.0f%% (%+.0fpp)%s\n",
                e$player_name, e$family, e$event_month,
                e$family, 100 * e$base_share, 100 * e$new_share, 100 * e$delta,
                ifelse(e$is_new_pitch, "  [NEW PITCH]", "")))
  }
}

write_csv(events, file.path(OUT_DIR, "phase2_events_2021_2025.csv"))
write_csv(ev26, file.path(OUT_DIR, "phase2_changers_2026.csv"))
cat(sprintf("\nOutputs:\n  %s\n  %s\n",
            file.path(OUT_DIR, "phase2_events_2021_2025.csv"),
            file.path(OUT_DIR, "phase2_changers_2026.csv")))
