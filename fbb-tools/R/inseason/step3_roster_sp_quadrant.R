# ── Step 3: Roster SP Quadrant + Streamonator ─────────────────────────────────
# PURPOSE: SP Skillz quadrant (skills vs results) PLUS streamonator start
#   recommendations for the current two-week window.
#
# QUADRANT:
#   X-axis: SP Skillz index (process/skills; 100=pool avg)
#   Y-axis: ERA+WHIP composite vs pool baseline (results/outcomes)
#   LEGIT         High SPZ + Good results  → trust it
#   BUY LOW       High SPZ + Bad results   → skills real; results will improve
#   SELL HIGH     Low SPZ  + Good results  → results masking weak skills
#   DROP CANDIDATE Low SPZ + Bad results   → both sides bad
#
# START RECOMMENDATION (per week, per start):
#   MUST START    streamer_score >= 107    (top ~20% of distribution)
#   COIN FLIP     95 <= score < 107        (middle ~55%)
#   AVOID         score < 95              (bottom ~25%)
#   Flags: ⚠PF park factor >= 115 | ⚠TR opponent offense >= 115

suppressPackageStartupMessages({
  library(dplyr); library(jsonlite); library(curl); library(httr)
})

base <- "/Users/ckaufman/Documents/New project/.claude/worktrees/frosty-moore/fbb-tools"
source(file.path(base, "R/sp_skillz.R"))
source(file.path(base, "R/modules/mod_sp_skillz.R"))
source(file.path(base, "R/modules/mod_team_rater.R"))
source(file.path(base, "R/modules/mod_sp_streamonator.R"))

norm_name <- function(x) tolower(trimws(gsub("[^a-z ]", "", iconv(x, to="ASCII//TRANSLIT"))))

# ── CONFIG ────────────────────────────────────────────────────────────────────
WEEK1_START    <- "2026-04-27"; WEEK1_END <- "2026-05-03"
WEEK2_START    <- "2026-05-04"; WEEK2_END <- "2026-05-10"
SPZ_THRESHOLD  <- 100
ERA_BASELINE   <- 4.20
WHIP_BASELINE  <- 1.28
MUST_START_THR <- 107
AVOID_THR      <- 95
PF_FLAG_THR    <- 115
TR_FLAG_THR    <- 115

# ── ROSTER SPs (update YTD each week) ─────────────────────────────────────────
roster_sp <- data.frame(
  name  = c("Tanner Bibee", "Kris Bubic", "Yusei Kikuchi", "J.R. Ritchie",
             "Kodai Senga", "Will Warren", "Joey Cantillo",
             "Nathan Eovaldi", "Framber Valdez"),
  team  = c("CLE", "KC",  "LAA", "ATL", "NYM", "NYY", "CLE", "TEX", "HOU"),
  era   = c(4.45,  4.08,  6.21,  2.57,  8.83,  2.59,  3.56,  5.79,  3.41),
  whip  = c(1.451, 1.116, 1.586, 1.000, 1.904, 1.149, 1.319, 1.469, 1.311),
  ip    = c(30.1,  28.2,  29.0,  7.0,   17.1,  31.1,  30.1,  32.2,  34.1),
  stringsAsFactors = FALSE
)
roster_sp$name_key <- norm_name(roster_sp$name)

# ── FETCH SP SKILLZ ───────────────────────────────────────────────────────────
cat("Fetching SP Skillz + probables + TR + PF...\n")
spz_std_fmt <- spz_gen_format(spz_gen_compute(
  spz_gen_parse(spz_gen_fetch(spz_gen_build_url(2026, 0))), ip_min = 10))
spz_l30_fmt <- spz_gen_format(spz_gen_compute(
  spz_gen_parse(spz_gen_fetch(spz_gen_build_url(2026, 3))), ip_min = 5))
spz_std_fmt$name_key <- norm_name(spz_std_fmt$Player)
spz_l30_fmt$name_key <- norm_name(spz_l30_fmt$Player)

# ── FETCH STREAMONATOR INPUTS ─────────────────────────────────────────────────
spz_std_raw <- spz_gen_compute(
  spz_gen_parse(spz_gen_fetch(spz_gen_build_url(2026, 0))), ip_min = 15)
spz_l30_raw <- spz_gen_compute(
  spz_gen_parse(spz_gen_fetch(spz_gen_build_url(2026, 3))), ip_min = 5)
prob   <- stream_parse_probables(stream_fg_fetch(STREAM_PROBABLES_URL))$data
tr_std <- trater_score(trater_parse(trater_fg_fetch(trater_build_url(2026, 0))))
tr_l30 <- trater_score(trater_parse(trater_fg_fetch(trater_build_url(2026, 3))))
pf     <- stream_load_pf()

stream_wk <- function(ws, we) {
  res <- stream_build(prob, spz_std_raw, spz_l30_raw, pf,
    tr_data = list(std = tr_std, l30 = tr_l30),
    week_start = ws, week_end = we,
    w_sp = 6, w_spz_std = 1, w_spz_l30 = 1,
    w_tr = 3, w_tr_std = 1, w_tr_l30 = 1, w_tr_vhand = 0, w_pf = 1)
  res$name_key <- norm_name(res$pitcher_name)
  res
}
s1 <- stream_wk(WEEK1_START, WEEK1_END)
s2 <- stream_wk(WEEK2_START, WEEK2_END)

start_label <- function(score) {
  if (is.na(score))         return("no start")
  if (score >= MUST_START_THR) return("MUST START")
  if (score >= AVOID_THR)   return("COIN FLIP")
  return("AVOID")
}

get_starts <- function(stream_df, nkey) {
  rows <- stream_df[stream_df$name_key == nkey, ]
  if (nrow(rows) == 0) return(list(n=0, score=NA, label="no start", matchups="—", flags=""))
  avg_score <- mean(rows$streamer_score, na.rm = TRUE)
  parts <- vapply(seq_len(nrow(rows)), function(j) {
    ha  <- rows$home_away[j]
    pfx <- if (!is.na(ha) && toupper(ha) == "H") "vs" else "at"
    sprintf("%s %s", pfx, rows$opponent_team[j])
  }, character(1))
  flags <- character(0)
  if (any(!is.na(rows$park_factor)  & rows$park_factor  >= PF_FLAG_THR)) flags <- c(flags, "⚠PF")
  if (any(!is.na(rows$team_rater)   & rows$team_rater   >= TR_FLAG_THR)) flags <- c(flags, "⚠TR")
  list(n       = nrow(rows),
       score   = round(avg_score, 1),
       label   = start_label(avg_score),
       matchups= paste(parts, collapse = " / "),
       flags   = paste(flags, collapse = ""))
}

# ── MERGE QUADRANT ────────────────────────────────────────────────────────────
roster_sp <- roster_sp %>%
  left_join(spz_std_fmt %>% select(name_key, spz_std = Score), by = "name_key") %>%
  left_join(spz_l30_fmt %>% select(name_key, spz_l30 = Score), by = "name_key") %>%
  mutate(
    spz_std     = as.numeric(spz_std),
    spz_l30     = as.numeric(spz_l30),
    spz_primary = coalesce(spz_l30, spz_std),
    era_vs_base  = ERA_BASELINE  - era,
    whip_vs_base = WHIP_BASELINE - whip,
    results_score = (era_vs_base + whip_vs_base * 2) / 3,
    good_skills  = !is.na(spz_primary) & spz_primary >= SPZ_THRESHOLD,
    good_results = results_score >= 0,
    quadrant = case_when(
       good_skills &  good_results ~ "LEGIT",
       good_skills & !good_results ~ "BUY LOW",
      !good_skills &  good_results ~ "SELL HIGH",
      !good_skills & !good_results ~ "DROP CANDIDATE",
      TRUE ~ "?"
    ),
    small_sample = ip < 20
  )

# ── OUTPUT ────────────────────────────────────────────────────────────────────
cat("\n=== STEP 3: YOLO ROSTER SPs — QUADRANT + STREAMONATOR ===\n")
cat(sprintf("Weeks: %s–%s | %s–%s\n", WEEK1_START, WEEK1_END, WEEK2_START, WEEK2_END))
cat(sprintf("Must start >= %.0f | Avoid < %.0f | PF/TR flags >= %.0f\n\n",
    MUST_START_THR, AVOID_THR, PF_FLAG_THR))

quadrant_order <- c("LEGIT", "BUY LOW", "SELL HIGH", "DROP CANDIDATE", "?")

for (q in quadrant_order) {
  sub <- roster_sp[roster_sp$quadrant == q, ]
  if (nrow(sub) == 0) next
  sub <- sub[order(-sub$spz_primary, na.last = TRUE), ]

  cat(sprintf("── %s ──────────────────────────────────────────────\n", q))
  cat(sprintf("  %-18s %5s %5s %5s %5s %5s | %-10s %-12s %-28s | %-10s %-12s %-28s\n",
    "Player", "IP", "ERA", "WHIP", "SPZ_S", "SPZ_L",
    "Wk1 Score", "Wk1 Start", "Wk1 Matchup",
    "Wk2 Score", "Wk2 Start", "Wk2 Matchup"))
  cat("  ", strrep("-", 130), "\n", sep = "")

  for (i in seq_len(nrow(sub))) {
    p  <- sub[i, ]
    w1 <- get_starts(s1, p$name_key)
    w2 <- get_starts(s2, p$name_key)
    ss_flag <- if (p$small_sample) "⚠SS" else ""

    cat(sprintf("  %-18s %5.1f %5.2f %5.3f %5s %5s | %10s %-12s %-28s | %10s %-12s %-28s  %s%s\n",
      p$name, p$ip, p$era, p$whip,
      ifelse(is.na(p$spz_std), "—", round(p$spz_std, 0)),
      ifelse(is.na(p$spz_l30), "—", round(p$spz_l30, 0)),
      ifelse(is.na(w1$score), "—", w1$score), w1$label, substr(paste(w1$matchups, w1$flags), 1, 28),
      ifelse(is.na(w2$score), "—", w2$score), w2$label, substr(paste(w2$matchups, w2$flags), 1, 28),
      ss_flag,
      if (w2$n >= 2) " 2⭐" else ""))
  }
  cat("\n")
}

cat("── SCALE ──────────────────────────────────────────────────────────────────\n")
cat("  SPZ: 100=pool avg | >110=elite | 90-100=serviceable | <90=avoid\n")
cat("  Score: 100=avg start | >=107 MUST START | <95 AVOID\n")
cat("  ⚠PF: park factor >= 115 (pitcher unfriendly) | ⚠TR: opp offense >= 115\n")
cat("  ⚠SS: small sample (IP < 20); treat SPZ and ERA/WHIP as noisy\n")
cat("  2⭐: two-start week\n")
