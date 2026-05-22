# ── Step 4: Waiver Wire SP Analysis ───────────────────────────────────────────
# PURPOSE: Find wire SPs with real skills (SP Skillz) and good matchups over
#   the next two weeks. Two-start weeks get a bonus flag (2⭐).
#
# INPUTS (update each week):
#   FANTRAX_PATH — downloaded from Fantrax > Players > export CSV (pitchers)
#   WEEK1_START / WEEK1_END / WEEK2_START / WEEK2_END — current two-week window
#
# DISPLAY: sorted by SPZ_L30 desc within each week's matchup rating bucket

suppressPackageStartupMessages({
  library(dplyr); library(readr); library(jsonlite); library(curl); library(httr)
})

base <- "/Users/ckaufman/Documents/New project/.claude/worktrees/frosty-moore/fbb-tools"
source(file.path(base, "R/sp_skillz.R"))
source(file.path(base, "R/modules/mod_sp_skillz.R"))
source(file.path(base, "R/modules/mod_team_rater.R"))
source(file.path(base, "R/modules/mod_sp_streamonator.R"))

norm_name <- function(x) tolower(trimws(gsub("[^a-z ]", "", iconv(x, to="ASCII//TRANSLIT"))))

# ── CONFIG ────────────────────────────────────────────────────────────────────
FANTRAX_PATH   <- "/Users/ckaufman/Downloads/Fantrax-Players-A Slog to Rigor Mortis(3).csv"
WEEK1_START    <- "2026-04-27"; WEEK1_END <- "2026-05-03"
WEEK2_START    <- "2026-05-04"; WEEK2_END <- "2026-05-10"
RKOVER_MAX     <- 650L     # wire SPs; tighter cap than hitters to avoid deep fringe
IP_MIN_STD     <- 10       # min IP for season SPZ
IP_MIN_L30     <- 5        # min IP for L30 SPZ
SPZ_SHOW_MIN   <- 90       # hide pitchers below this SPZ (both std and l30 must be >= or NA)
MUST_START_THR <- 107
AVOID_THR      <- 95
PF_FLAG_THR    <- 115
TR_FLAG_THR    <- 115

# ── LOAD FANTRAX PITCHER EXPORT ───────────────────────────────────────────────
fantrax <- read_csv(FANTRAX_PATH, show_col_types = FALSE) %>%
  mutate(
    name_key = norm_name(Player),
    RkOv     = suppressWarnings(as.integer(RkOv))
  ) %>%
  filter(
    Position == "SP",
    Status   == "FA",
    !is.na(RkOv), RkOv <= RKOVER_MAX
  ) %>%
  mutate(
    IP   = suppressWarnings(as.numeric(IP)),
    ERA  = suppressWarnings(as.numeric(ERA)),
    WHIP = suppressWarnings(as.numeric(WHIP))
  ) %>%
  arrange(RkOv)

cat(sprintf("Wire SPs from Fantrax (SP, FA, RkOv <= %d): %d pitchers\n", RKOVER_MAX, nrow(fantrax)))

# ── FETCH SP SKILLZ ───────────────────────────────────────────────────────────
cat("Fetching SP Skillz + probables + TR + PF...\n")
spz_std_fmt <- spz_gen_format(spz_gen_compute(
  spz_gen_parse(spz_gen_fetch(spz_gen_build_url(2026, 0))), ip_min = IP_MIN_STD))
spz_l30_fmt <- spz_gen_format(spz_gen_compute(
  spz_gen_parse(spz_gen_fetch(spz_gen_build_url(2026, 3))), ip_min = IP_MIN_L30))
spz_std_fmt$name_key <- norm_name(spz_std_fmt$Player)
spz_l30_fmt$name_key <- norm_name(spz_l30_fmt$Player)

# Raw for streamonator
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
    tr_data    = list(std = tr_std, l30 = tr_l30),
    week_start = ws, week_end = we,
    w_sp = 6, w_spz_std = 1, w_spz_l30 = 1,
    w_tr = 3, w_tr_std = 1, w_tr_l30 = 1, w_tr_vhand = 0, w_pf = 1)
  res$name_key <- norm_name(res$pitcher_name)
  res
}
s1 <- stream_wk(WEEK1_START, WEEK1_END)
s2 <- stream_wk(WEEK2_START, WEEK2_END)

start_label <- function(score) {
  if (is.na(score))              return("no start")
  if (score >= MUST_START_THR)   return("MUST START")
  if (score >= AVOID_THR)        return("COIN FLIP")
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
  if (any(!is.na(rows$park_factor) & rows$park_factor >= PF_FLAG_THR)) flags <- c(flags, "⚠PF")
  if (any(!is.na(rows$team_rater)  & rows$team_rater  >= TR_FLAG_THR)) flags <- c(flags, "⚠TR")
  list(n       = nrow(rows),
       score   = round(avg_score, 1),
       label   = start_label(avg_score),
       matchups= paste(parts, collapse = " / "),
       flags   = paste(flags, collapse = ""))
}

# ── MERGE SPZ ONTO FANTRAX LIST ───────────────────────────────────────────────
wire <- fantrax %>%
  left_join(spz_std_fmt %>% select(name_key, spz_std = Score), by = "name_key") %>%
  left_join(spz_l30_fmt %>% select(name_key, spz_l30 = Score), by = "name_key") %>%
  mutate(
    spz_std     = suppressWarnings(as.numeric(spz_std)),
    spz_l30     = suppressWarnings(as.numeric(spz_l30)),
    spz_primary = coalesce(spz_l30, spz_std),
    small_sample = !is.na(IP) & IP < 20
  ) %>%
  filter(
    # Keep if either SPZ is present and meets threshold, or if both are NA (not enough IP yet)
    is.na(spz_primary) | spz_primary >= SPZ_SHOW_MIN
  )

cat(sprintf("After SPZ filter (>= %d or no data): %d pitchers\n\n", SPZ_SHOW_MIN, nrow(wire)))

# ── OUTPUT ────────────────────────────────────────────────────────────────────
cat(sprintf("=== STEP 4: WIRE SPs — SP Skillz + 2-Week Streamonator (RkOv <= %d, FA only) ===\n",
    RKOVER_MAX))
cat(sprintf("Weeks: %s–%s | %s–%s\n", WEEK1_START, WEEK1_END, WEEK2_START, WEEK2_END))
cat(sprintf("Must start >= %.0f | Avoid < %.0f | PF/TR flags >= %.0f\n\n",
    MUST_START_THR, AVOID_THR, PF_FLAG_THR))

# Sort by best week 1 streamer score descending (among those who have starts),
# then spz_primary desc
wire_ordered <- wire %>%
  rowwise() %>%
  mutate(
    w1_score = {
      r <- get_starts(s1, name_key)
      r$score
    },
    w2_score = {
      r <- get_starts(s2, name_key)
      r$score
    }
  ) %>%
  ungroup() %>%
  mutate(
    best_score = pmax(w1_score, w2_score, na.rm = TRUE)
  ) %>%
  arrange(desc(best_score), desc(spz_primary))

cat(sprintf("  %-22s %-4s %5s %5s %5s | %10s %-12s %-30s | %10s %-12s %-30s\n",
    "Player", "Team", "IP", "SPZ_S", "SPZ_L",
    "Wk1 Score", "Wk1 Start", "Wk1 Matchup",
    "Wk2 Score", "Wk2 Start", "Wk2 Matchup"))
cat("  ", strrep("-", 138), "\n", sep = "")

for (i in seq_len(nrow(wire_ordered))) {
  p  <- wire_ordered[i, ]
  w1 <- get_starts(s1, p$name_key)
  w2 <- get_starts(s2, p$name_key)

  # skip if no start in either week
  if (w1$n == 0 && w2$n == 0) next

  ss_flag <- if (isTRUE(p$small_sample)) "⚠SS" else ""
  two_star <- if (w1$n >= 2 || w2$n >= 2) " 2⭐" else ""

  cat(sprintf("  %-22s %-4s %5s %5s %5s | %10s %-12s %-30s | %10s %-12s %-30s  %s%s\n",
    p$Player, p$Team,
    ifelse(is.na(p$IP),      "—", sprintf("%.1f", p$IP)),
    ifelse(is.na(p$spz_std), "—", round(p$spz_std, 0)),
    ifelse(is.na(p$spz_l30), "—", round(p$spz_l30, 0)),
    ifelse(is.na(w1$score),  "—", w1$score), w1$label,
    substr(paste(w1$matchups, w1$flags), 1, 30),
    ifelse(is.na(w2$score),  "—", w2$score), w2$label,
    substr(paste(w2$matchups, w2$flags), 1, 30),
    ss_flag, two_star))
}

cat("\n── SCALE ──────────────────────────────────────────────────────────────────\n")
cat("  SPZ: 100=pool avg | >110=elite | 90-100=serviceable | <90=avoid\n")
cat("  Score: 100=avg start | >=107 MUST START | <95 AVOID\n")
cat("  ⚠PF: park factor >= 115 | ⚠TR: opp offense >= 115\n")
cat("  ⚠SS: small sample (IP < 20)\n")
cat("  2⭐: two-start week (either week)\n")
