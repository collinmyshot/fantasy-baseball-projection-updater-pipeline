#!/usr/bin/env Rscript
# streamonator_defense_analysis.R
#
# Question: does adding the streamed pitcher's OWN-TEAM in-season defense
# (UZR+DRS+OAA composite) to the Streamonator improve the Park Factor component
# enough that the combined "run-prevention environment" weight should exceed 1
# in the 6:3:1 (SP Skillz : Team Rater : Park Factor) system?
#
# Design decisions (confirmed with user 2026-06-22):
#   - Defense is the PITCHER'S OWN team (fielders behind him), joined on
#     pitcher_team + season.  NOT inverted: better defense -> higher index ->
#     helps the pitcher (same direction as SP Skillz).
#   - Two tests:
#       (b) Independent 4th component:  grid search SP : TR : PF : Def
#       (a) Merged environment:         PF + Def -> one component E, grid SP:TR:E
#
# Baseline reference = the article's validation sample: starts EXCLUDING
# SP-Skillz placeholders, with a valid 6:3:1 score (N ~ 22,917, 2021-2025).
# GSM is recomputed 0-4 (no Win, sliding ER, WHIP<=1.18) to match mod_gsm.R.
#
# ── ANSWER (run 2026-06-22): NO. Defense does not earn a weight bump. ────────
#   Defense is a small but genuinely ORTHOGONAL positive signal:
#     Spearman vs GSM ~0.045 per start (park-only is ~0.033)
#     bottom-third-defense starts 52.0% good vs top-third 55.6%
#   Folding it into the environment component lifts that component's rho
#   0.2928 -> 0.2947 (50/50 park:def), but the component's OPTIMAL WEIGHT STAYS
#   AT 1. So it does not justify raising PF above 1 in 6:3:1.
#   Best 4-way is 6:2:1:1 (the Def weight comes out of Team Rater, not PF) for
#   +0.003 Spearman; the >105 bucket's %good is unchanged at ~69.8%.
#   Verdict: interesting, not shippable. Not adopted.
#
#   Follow-ups that came out of this: streamonator_defense_metric_decomp.R
#   (is 1:1:1 the right internal blend?), streamonator_gb_defense_interaction.R
#   (does it matter more for groundballers?), streamonator_shape_diagnostic.R
#   (is the relationship linear?).
#
# Fully cached; no API calls.  Usage: Rscript scripts/validation_tuning/streamonator_defense_analysis.R

CACHE_DIR <- file.path("data", "processed", "streamonator_weight_analysis")
DEF_FILE  <- file.path("data", "manual", "team_defense_2015_2025.csv")
SEASONS   <- 2021:2025

# Same team normalization the pipeline uses (ARI->AZ, WSN->WSH, OAK->ATH, ...)
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

# ── 1. Load starts + recompute authoritative GSM (0-4) ────────────────────────
message("Loading starts 2021-2025...")
s <- do.call(rbind, lapply(SEASONS, function(yr) {
  d <- read.csv(file.path(CACHE_DIR, sprintf("starts_%d.csv", yr)), stringsAsFactors = FALSE)
  d$season <- yr; d
}))
s$pitcher_team  <- norm_team(s$pitcher_team)
s$opponent_team <- norm_team(s$opponent_team)

s$whip <- ifelse(!is.na(s$ip) & s$ip > 0, (s$h + s$bb) / s$ip, Inf)
ip_ok   <- !is.na(s$ip) & s$ip >= 5
k_ok    <- !is.na(s$k) & !is.na(s$ip) & s$k >= (floor(s$ip) - 1)
er_ok   <- !is.na(s$er) & !is.na(s$ip) & (
  (s$ip >= 6 & s$er <= 2) | (s$ip >= 5 & s$ip < 6 & s$er <= 3) |
  (s$ip >= 4 & s$ip < 5 & s$er <= 2) | (s$ip < 4 & s$er <= 1))
whip_ok <- !is.na(s$whip) & s$whip <= 1.18
s$gsm <- as.integer(ip_ok) + as.integer(k_ok) + as.integer(er_ok) + as.integer(whip_ok)

# ── 2. Build own-team in-season defense composite + index ─────────────────────
message("Building own-team defense composite...")
def <- read.csv(DEF_FILE, stringsAsFactors = FALSE)
def <- def[def$season %in% SEASONS, ]
def$team_norm <- norm_team(def$team)
# z within season, then mean of available metrics (matches build_defense_composite)
z_by_season <- function(x, season) {
  ave(x, season, FUN = function(v) {
    mu <- mean(v, na.rm = TRUE); sg <- sd(v, na.rm = TRUE)
    if (is.na(sg) || sg == 0) return(rep(NA_real_, length(v)))
    (v - mu) / sg
  })
}
def$z_oaa <- z_by_season(def$oaa, def$season)
def$z_drs <- z_by_season(def$drs, def$season)
def$z_uzr <- z_by_season(def$uzr, def$season)
def$def_comp <- rowMeans(cbind(def$z_oaa, def$z_drs, def$z_uzr), na.rm = TRUE)
def$def_comp[!is.finite(def$def_comp)] <- NA_real_

# Join to each start on (pitcher_team, season)
key <- paste(s$pitcher_team, s$season)
dkey <- paste(def$team_norm, def$season)
s$def_comp <- def$def_comp[match(key, dkey)]
match_rate <- mean(!is.na(s$def_comp))
message(sprintf("  Defense join: %.1f%% of starts matched (%d unmatched)",
                100 * match_rate, sum(is.na(s$def_comp))))

# ── 3. Define the baseline validation sample (matches the article) ────────────
score_wavg <- function(mat, w) {
  # mat: rows x k components (already directionally correct, ~100 scale)
  # w:   length-k weights.  NA-safe weighted mean per row.
  pres <- !is.na(mat)
  num  <- rowSums(sweep(ifelse(pres, mat, 0), 2, w, `*`))
  den  <- as.vector(pres %*% w)
  ifelse(den > 0, num / den, NA_real_)
}
SP  <- s$sp_skillz_index
TRi <- s$team_rater_inv          # already (200 - team_rater)
PFi <- s$park_factor_inv         # already (200 - park_factor)
s$score_631 <- score_wavg(cbind(SP, TRi, PFi), c(6, 3, 1))

# Defense index: 100-centered, sd 10, higher = better D = better for pitcher
s$def_z     <- as.numeric(scale(s$def_comp))
s$def_index <- 100 + s$def_z * 10

base <- s[!s$spz_placeholder & !is.na(s$score_631) & !is.na(s$gsm) & !is.na(s$def_index), ]
message(sprintf("\nBaseline sample (non-placeholder, valid score & defense): N = %d", nrow(base)))
message(sprintf("  Overall %%Good=%.1f  Spearman(6:3:1, GSM)=%.4f",
                100 * mean(base$gsm >= 3), cor(base$score_631, base$gsm, method = "spearman")))

bucket_good <- function(score, gsm) {
  # % good among starts scoring >105 (the headline "Start" bucket)
  sel <- score > 105
  c(n = sum(sel), pct_good = 100 * mean(gsm[sel] >= 3))
}
b0 <- bucket_good(base$score_631, base$gsm)
message(sprintf("  >105 Start bucket: n=%d  %%Good=%.1f%%  (article: 69.9%%)", b0["n"], b0["pct_good"]))

# ── 4. FOUNDATIONAL: does own-team defense predict GSM at all? ─────────────────
message("\n==================================================================")
message("  FOUNDATIONAL — standalone signal of own-team defense")
message("==================================================================")
rho_def <- cor(base$def_index, base$gsm, method = "spearman")
message(sprintf("  Spearman(def_index, GSM) = %.4f", rho_def))
message(sprintf("  (for reference: SP Skillz=%.4f, 200-TR=%.4f, 200-PF=%.4f)",
                cor(base$sp_skillz_index, base$gsm, method = "spearman"),
                cor(base$team_rater_inv, base$gsm, method = "spearman"),
                cor(base$park_factor_inv, base$gsm, method = "spearman", use = "complete.obs")))

# Defense tercile %good
ter <- cut(base$def_index, breaks = quantile(base$def_index, c(0, 1/3, 2/3, 1), na.rm = TRUE),
           labels = c("Bot 3rd D", "Mid 3rd D", "Top 3rd D"), include.lowest = TRUE)
message("\n  Good-start rate by own-team defense tercile:")
for (t in levels(ter)) {
  sel <- ter == t
  message(sprintf("    %-10s  n=%5d  %%Good=%.1f  %%Bad=%.1f",
                  t, sum(sel), 100 * mean(base$gsm[sel] >= 3), 100 * mean(base$gsm[sel] <= 1)))
}

# Collinearity with existing components
message("\n  Correlation of def_index with existing components (Pearson):")
message(sprintf("    vs SP Skillz = %.3f | vs 200-TR = %.3f | vs 200-PF = %.3f",
                cor(base$def_index, base$sp_skillz_index, use = "complete.obs"),
                cor(base$def_index, base$team_rater_inv, use = "complete.obs"),
                cor(base$def_index, base$park_factor_inv, use = "complete.obs")))

# ── 5. TEST (b): independent 4th component  SP : TR : PF : Def ─────────────────
message("\n==================================================================")
message("  TEST (b) — independent 4th component: SP : TR : PF : Def")
message("==================================================================")
g4 <- expand.grid(w_sp = 0:10, w_tr = 0:10, w_pf = 0:10, w_def = 0:10)
g4 <- g4[rowSums(g4) == 10, ]; rownames(g4) <- NULL
M <- cbind(base$sp_skillz_index, base$team_rater_inv, base$park_factor_inv, base$def_index)
res4 <- do.call(rbind, lapply(seq_len(nrow(g4)), function(j) {
  w <- as.numeric(g4[j, ]) / 10
  sc <- score_wavg(M, w)
  ok <- !is.na(sc)
  data.frame(w_sp = w[1], w_tr = w[2], w_pf = w[3], w_def = w[4],
             spearman = cor(sc[ok], base$gsm[ok], method = "spearman"),
             pct_good_105 = 100 * mean(base$gsm[ok & sc > 105] >= 3),
             n105 = sum(ok & sc > 105))
}))
res4 <- res4[order(-res4$spearman), ]; rownames(res4) <- NULL

message("\n  Top 12 by Spearman:")
message(sprintf("  %5s %5s %5s %6s | %8s %10s %6s", "SP","TR","PF","Def","Spearman","%Good>105","n105"))
message("  ", strrep("-", 58))
for (i in 1:12) { r <- res4[i, ]
  message(sprintf("  %5.1f %5.1f %5.1f %6.1f | %8.4f %9.1f%% %6d",
                  r$w_sp, r$w_tr, r$w_pf, r$w_def, r$spearman, r$pct_good_105, r$n105)) }

# Baseline 6:3:1:0 row + best, with combined environment weight (pf+def)
base_row <- res4[abs(res4$w_sp-.6)<1e-6 & abs(res4$w_tr-.3)<1e-6 & abs(res4$w_pf-.1)<1e-6 & abs(res4$w_def)<1e-6, ]
best_row <- res4[1, ]
message(sprintf("\n  Baseline 6:3:1:0      -> Spearman=%.4f  %%Good>105=%.1f%%", base_row$spearman, base_row$pct_good_105))
message(sprintf("  Best overall          -> SP=%.1f TR=%.1f PF=%.1f Def=%.1f  Spearman=%.4f  %%Good>105=%.1f%%",
                best_row$w_sp, best_row$w_tr, best_row$w_pf, best_row$w_def, best_row$spearman, best_row$pct_good_105))
message(sprintf("  Combined environment weight at optimum (PF+Def) = %.1f  (baseline PF-only = 0.1)",
                best_row$w_pf + best_row$w_def))
message(sprintf("  Spearman delta vs baseline = %+.4f", best_row$spearman - base_row$spearman))

# Best holding SP:TR at 6:3 (i.e. only re-allocating the bottom 0.1 between PF/Def, or growing it)
message("\n  Holding the SP-vs-TR balance, how should the non-skill bucket split?")
for (env in seq(0.1, 0.4, by = 0.1)) {
  # keep SP:TR = 2:1 ratio, give 'env' total to PF+Def, search internal split
  sub <- res4[abs((res4$w_sp/(res4$w_sp+res4$w_tr)) - 2/3) < 0.02 &
              abs(res4$w_pf + res4$w_def - env) < 1e-6, ]
  if (nrow(sub) == 0) next
  sub <- sub[order(-sub$spearman), ][1, ]
  message(sprintf("    env=%.1f (SP=%.1f TR=%.1f): best PF=%.1f Def=%.1f  Spearman=%.4f",
                  env, sub$w_sp, sub$w_tr, sub$w_pf, sub$w_def, sub$spearman))
}

# ── 6. TEST (a): merged environment component  PF + Def -> E ──────────────────
message("\n==================================================================")
message("  TEST (a) — merged environment component: SP : TR : (PF+Def)")
message("==================================================================")
# Build E at several internal park:def blends (on z-scale so each contributes
# its stated share of variance). a = park share, (1-a) = defense share.
z_pf  <- as.numeric(scale(base$park_factor_inv))
z_def <- as.numeric(scale(base$def_index))
g3 <- expand.grid(w_sp = 0:10, w_tr = 0:10, w_e = 0:10)
g3 <- g3[rowSums(g3) == 10 & g3$w_e > 0, ]; rownames(g3) <- NULL

for (a in c(1.0, 0.75, 0.5, 0.25, 0.0)) {
  e_raw <- a * ifelse(is.na(z_pf), 0, z_pf) + (1 - a) * z_def
  # if pf missing, fall back to defense-only for that row
  e_raw[is.na(z_pf)] <- z_def[is.na(z_pf)]
  E <- 100 + as.numeric(scale(e_raw)) * 10
  ME <- cbind(base$sp_skillz_index, base$team_rater_inv, E)
  r3 <- do.call(rbind, lapply(seq_len(nrow(g3)), function(j) {
    w <- as.numeric(g3[j, ]) / 10
    sc <- score_wavg(ME, w); ok <- !is.na(sc)
    data.frame(w_sp = w[1], w_tr = w[2], w_e = w[3],
               spearman = cor(sc[ok], base$gsm[ok], method = "spearman"))
  }))
  r3 <- r3[order(-r3$spearman), ]
  top <- r3[1, ]
  # the 6:3:1-equivalent row (w_e = 0.1)
  ref <- r3[abs(r3$w_sp-.6)<1e-6 & abs(r3$w_tr-.3)<1e-6 & abs(r3$w_e-.1)<1e-6, ]
  lbl <- sprintf("park %.0f%% / def %.0f%%", 100*a, 100*(1-a))
  message(sprintf("  E = %-22s | best: SP=%.1f TR=%.1f E=%.1f rho=%.4f | 6:3:1-equiv rho=%.4f",
                  lbl, top$w_sp, top$w_tr, top$w_e, top$spearman, ref$spearman))
}
message("\n  (a=park 100%% reproduces the PF-only baseline; lower a folds in defense.)")
message("\nDone.")
