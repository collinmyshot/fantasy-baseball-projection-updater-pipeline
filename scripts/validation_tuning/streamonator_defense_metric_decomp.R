#!/usr/bin/env Rscript
# streamonator_defense_metric_decomp.R
#
# Question (from user): should the team-defense composite weight OAA / DRS / UZR
# 1:1:1, or does a different internal weighting predict pitcher good-starts better?
#
# Honest grain = TEAM-SEASON (n=150 for 2021-2025; UZR present for 120 = 2021-24).
# The 22,917-start sample contains only 150 unique defense values, so tuning
# internal weights against per-start rows is pseudo-replicated overfit bait.
#
# Primary signal = defense correlation with a team-season's good-start rate
# NET OF STAFF QUALITY (residualize good_start_rate on the team's avg SP Skillz),
# so "good orgs have good pitching AND good defense" doesn't inflate it.
#
# ── ANSWER (run 2026-06-22): keep 1:1:1. ────────────────────────────────────
#   At team-season grain, staff-adjusted, the composite correlates ~0.323
#   (Spearman) with good-start rate — note this is MUCH stronger than the 0.045
#   the per-start pooled view suggests, because per-start rows pseudo-replicate
#   150 distinct defense values.
#   Equal weighting beats every alternative tried:
#     1:1:1 composite 0.323 | OAA alone 0.288 | DRS 0.272 | UZR 0.256
#     dropping the "noisy" UZR (OAA+DRS) 0.308 — i.e. dropping it HURTS
#   Why: the three metrics carry partially-independent noise (DRS<->UZR r=0.72,
#   but OAA is distinct at r~0.55), so averaging all three cancels error.
#   HONEST FRAMING: variant gaps are inside n=150 noise. The claim is "no
#   evidence to move off 1:1:1", NOT "1:1:1 proven optimal."
#   (Per-start, DRS alone 0.058 edges the composite 0.045 — that's the
#   pseudo-replication artifact, not a reason to switch.)
#
# Fully cached; no API calls.  Usage: Rscript scripts/validation_tuning/streamonator_defense_metric_decomp.R

CACHE_DIR <- file.path("data", "processed", "streamonator_weight_analysis")
DEF_FILE  <- file.path("data", "manual", "team_defense_2015_2025.csv")
SEASONS   <- 2021:2025

TEAM_MAP <- c(
  ARI="AZ", ARZ="AZ", AZ="AZ", ATH="ATH", OAK="ATH", BAL="BAL", BOS="BOS",
  CHC="CHC", CHN="CHC", CHW="CHW", CHA="CHW", CWS="CHW", CIN="CIN", CLE="CLE",
  CLV="CLE", COL="COL", DET="DET", HOU="HOU", KC="KCR", KCR="KCR", LAA="LAA",
  LAD="LAD", LA="LAD", MIA="MIA", FLA="MIA", MIL="MIL", MIN="MIN", NYM="NYM",
  NYY="NYY", PHI="PHI", PIT="PIT", SD="SDP", SDP="SDP", SEA="SEA", SF="SFG",
  SFG="SFG", STL="STL", TB="TBR", TBR="TBR", TAM="TBR", TEX="TEX", TOR="TOR",
  WSH="WSH", WSN="WSH", WAS="WSH"
)
norm_team <- function(x) {
  out <- TEAM_MAP[toupper(trimws(as.character(x)))]
  out[is.na(out)] <- toupper(trimws(as.character(x)))[is.na(out)]
  unname(out)
}

# ── Load starts, recompute GSM, build baseline sample ─────────────────────────
s <- do.call(rbind, lapply(SEASONS, function(yr) {
  d <- read.csv(file.path(CACHE_DIR, sprintf("starts_%d.csv", yr)), stringsAsFactors = FALSE)
  d$season <- yr; d
}))
s$pitcher_team <- norm_team(s$pitcher_team)
s$whip <- ifelse(!is.na(s$ip) & s$ip > 0, (s$h + s$bb) / s$ip, Inf)
ip_ok   <- !is.na(s$ip) & s$ip >= 5
k_ok    <- !is.na(s$k) & !is.na(s$ip) & s$k >= (floor(s$ip) - 1)
er_ok   <- !is.na(s$er) & !is.na(s$ip) & (
  (s$ip >= 6 & s$er <= 2) | (s$ip >= 5 & s$ip < 6 & s$er <= 3) |
  (s$ip >= 4 & s$ip < 5 & s$er <= 2) | (s$ip < 4 & s$er <= 1))
whip_ok <- !is.na(s$whip) & s$whip <= 1.18
s$gsm <- as.integer(ip_ok) + as.integer(k_ok) + as.integer(er_ok) + as.integer(whip_ok)
base <- s[!s$spz_placeholder & !is.na(s$gsm), ]

# ── Aggregate to team-season ──────────────────────────────────────────────────
ts <- aggregate(cbind(good = base$gsm >= 3, avg_gsm = base$gsm, sp = base$sp_skillz_index,
                      one = 1) ~ pitcher_team + season, data = base, FUN = mean)
n_ct <- aggregate(one ~ pitcher_team + season, data = transform(base, one = 1), FUN = length)
ts$n_starts <- n_ct$one[match(paste(ts$pitcher_team, ts$season), paste(n_ct$pitcher_team, n_ct$season))]
names(ts)[names(ts) == "good"] <- "good_rate"
names(ts)[names(ts) == "sp"]   <- "mean_spz"
ts$good_rate <- 100 * ts$good_rate

# ── Join defense metrics, z within season ─────────────────────────────────────
def <- read.csv(DEF_FILE, stringsAsFactors = FALSE)
def <- def[def$season %in% SEASONS, ]
def$team_norm <- norm_team(def$team)
z_by_season <- function(x, season) ave(x, season, FUN = function(v) {
  mu <- mean(v, na.rm = TRUE); sg <- sd(v, na.rm = TRUE)
  if (is.na(sg) || sg == 0) rep(NA_real_, length(v)) else (v - mu) / sg })
def$z_oaa <- z_by_season(def$oaa, def$season)
def$z_drs <- z_by_season(def$drs, def$season)
def$z_uzr <- z_by_season(def$uzr, def$season)
mi <- match(paste(ts$pitcher_team, ts$season), paste(def$team_norm, def$season))
ts$z_oaa <- def$z_oaa[mi]; ts$z_drs <- def$z_drs[mi]; ts$z_uzr <- def$z_uzr[mi]

cat(sprintf("Team-seasons: %d  (UZR present: %d)\n", nrow(ts), sum(!is.na(ts$z_uzr))))
cat(sprintf("good_rate range %.1f-%.1f  mean %.1f | starts/team-season mean %.0f\n\n",
            min(ts$good_rate), max(ts$good_rate), mean(ts$good_rate), mean(ts$n_starts)))

# ── Staff-adjusted good-rate (residual after removing team avg SP Skillz) ──────
ts$good_resid <- residuals(lm(good_rate ~ mean_spz, data = ts))

corr2 <- function(x, y) {
  ok <- is.finite(x) & is.finite(y)
  c(pearson = cor(x[ok], y[ok]), spearman = cor(x[ok], y[ok], method = "spearman"), n = sum(ok))
}

cat("================================================================\n")
cat("  1. STANDALONE: each metric vs team-season good-start rate\n")
cat("================================================================\n")
cat(sprintf("  %-10s %8s %9s %5s | %8s %9s   (staff-adjusted)\n",
            "metric", "Pearson", "Spearman", "n", "Pearson", "Spearman"))
for (m in c("z_oaa", "z_drs", "z_uzr")) {
  raw <- corr2(ts[[m]], ts$good_rate); adj <- corr2(ts[[m]], ts$good_resid)
  cat(sprintf("  %-10s %8.3f %9.3f %5d | %8.3f %9.3f\n",
              m, raw["pearson"], raw["spearman"], raw["n"], adj["pearson"], adj["spearman"]))
}

cat("\n================================================================\n")
cat("  2. INTERCORRELATION among the three metrics (how redundant?)\n")
cat("================================================================\n")
zmat <- ts[, c("z_oaa", "z_drs", "z_uzr")]
print(round(cor(zmat, use = "pairwise.complete.obs"), 3))

cat("\n================================================================\n")
cat("  3. COMPOSITE VARIANTS vs good-start rate (raw & staff-adjusted)\n")
cat("================================================================\n")
mk <- function(w_oaa, w_drs, w_uzr) {
  M <- cbind(ts$z_oaa * w_oaa, ts$z_drs * w_drs, ts$z_uzr * w_uzr)
  wpres <- cbind(!is.na(ts$z_oaa) * w_oaa, !is.na(ts$z_drs) * w_drs, !is.na(ts$z_uzr) * w_uzr)
  num <- rowSums(M, na.rm = TRUE); den <- rowSums(wpres)
  ifelse(den > 0, num / den, NA_real_)
}
variants <- list(
  "1:1:1 (current)"   = c(1, 1, 1),
  "OAA only"          = c(1, 0, 0),
  "DRS only"          = c(0, 1, 0),
  "UZR only"          = c(0, 0, 1),
  "OAA+DRS (no UZR)"  = c(1, 1, 0),
  "OAA+DRS 2:1"       = c(2, 1, 0),
  "OAA+DRS 1:2"       = c(1, 2, 0)
)
cat(sprintf("  %-18s %8s %9s | %8s %9s  (staff-adj)\n",
            "composite", "Pearson", "Spearman", "Pearson", "Spearman"))
for (nm in names(variants)) {
  w <- variants[[nm]]; comp <- mk(w[1], w[2], w[3])
  raw <- corr2(comp, ts$good_rate); adj <- corr2(comp, ts$good_resid)
  cat(sprintf("  %-18s %8.3f %9.3f | %8.3f %9.3f\n",
              nm, raw["pearson"], raw["spearman"], adj["pearson"], adj["spearman"]))
}

cat("\n  Note: n=150 team-seasons (120 with UZR). With a signal this small,\n")
cat("  rank-order differences across variants are within sampling noise —\n")
cat("  treat any single 'winner' skeptically; look for a consistent story.\n")

# ── Start-level cross-check (pseudo-replicated; for comparison only) ───────────
cat("\n================================================================\n")
cat("  4. START-LEVEL cross-check (Spearman vs GSM, n=22,917)\n")
cat("================================================================\n")
mi2 <- match(paste(base$pitcher_team, base$season), paste(def$team_norm, def$season))
for (m in c("z_oaa", "z_drs", "z_uzr")) {
  v <- def[[m]][mi2]; ok <- is.finite(v) & is.finite(base$gsm)
  cat(sprintf("  %-10s Spearman = %.4f  (n=%d)\n", m, cor(v[ok], base$gsm[ok], method = "spearman"), sum(ok)))
}
comp_start <- mk2 <- {
  M <- cbind(def$z_oaa[mi2], def$z_drs[mi2], def$z_uzr[mi2]); rowMeans(M, na.rm = TRUE) }
ok <- is.finite(comp_start) & is.finite(base$gsm)
cat(sprintf("  %-10s Spearman = %.4f  (n=%d)\n", "1:1:1", cor(comp_start[ok], base$gsm[ok], method = "spearman"), sum(ok)))
cat("\nDone.\n")
