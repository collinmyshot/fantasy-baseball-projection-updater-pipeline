# ── Step 2: Waiver Wire Hitter Analysis ───────────────────────────────────────
# PURPOSE: Find wire hitters who are RoS upgrades over rostered players,
#   and identify hitter streamonator matchup plays for the current week.
#
# INPUTS (update each week):
#   FANTRAX_PATH — downloaded from Fantrax > Players > export CSV (hitters)
#   XLSX_PATH    — same app export used in Step 1 (OBP-scored, with positions)
#   WEEK_START / WEEK_END — current scoring week

suppressPackageStartupMessages({
  library(dplyr); library(readr); library(openxlsx)
  library(jsonlite); library(curl); library(httr); library(shiny)
})

base <- tryCatch({
  normalizePath(file.path(dirname(rstudioapi::getSourceEditorContext()$path), "../.."))
}, error = function(e) {
  args <- commandArgs(trailingOnly = FALSE)
  f    <- grep("^--file=", args, value = TRUE)
  if (length(f) > 0) normalizePath(file.path(dirname(sub("^--file=", "", f[1])), "../.."))
  else file.path(getwd(), "fbb-tools")
})
source(file.path(base, "R/sp_skillz.R"))
source(file.path(base, "R/modules/mod_sp_skillz.R"))
source(file.path(base, "R/modules/mod_team_rater.R"))
source(file.path(base, "R/modules/mod_sp_streamonator.R"))
source(file.path(base, "R/modules/mod_hit_streamonator.R"))

norm_name <- function(x) tolower(trimws(gsub("[^a-z ]", "", iconv(x, to="ASCII//TRANSLIT"))))

# ── CONFIG ────────────────────────────────────────────────────────────────────
FANTRAX_PATH  <- "/Users/ckaufman/Downloads/Fantrax-Players-A Slog to Rigor Mortis(2).csv"
XLSX_PATH     <- "/Users/ckaufman/Downloads/auction_values_roto_2026-04-26(5).xlsx"
for (.p in c(FANTRAX_PATH, XLSX_PATH)) {
  if (!file.exists(.p)) stop(paste0(
    "File not found: ", .p, "\n",
    "  Update the path above with the current filename.\n",
    "  If you switched machines (Mac ↔ Windows), also update the path prefix:\n",
    "    Mac:     /Users/ckaufman/Downloads/<filename>\n",
    "    Windows: C:/Users/Collin/Downloads/<filename>"
  ))
}
rm(.p)
WEEK_START    <- "2026-04-27"
WEEK_END      <- "2026-05-03"
RKOVER_MAX    <- 1000L   # exclude deep fringe (also catches name-collision phantoms)
MIN_DV        <- 3.0     # minimum $val to show (set near weakest drop candidate)
TOP_N         <- 20L     # max rows to display

# ── LOAD APP EXPORT + BUILD POSITIONAL RANKS ──────────────────────────────────
h <- read.xlsx(XLSX_PATH, sheet = "Hitters")
h$playerid     <- as.character(h$playerid)
h$name_key     <- norm_name(h$name)
h_sorted       <- h %>% arrange(desc(dollar_value))
h_sorted$overall_rank <- seq_len(nrow(h_sorted))

pos_pool <- function(df, pattern) {
  sub <- df[!is.na(df$positions) & grepl(pattern, df$positions, ignore.case = TRUE), ]
  sub <- sub[order(-sub$dollar_value), ]
  data.frame(name_key = sub$name_key, pr = seq_len(nrow(sub)), stringsAsFactors = FALSE)
}
pr_C  <- pos_pool(h_sorted, "\\bC\\b")
pr_1B <- pos_pool(h_sorted, "1B")
pr_2B <- pos_pool(h_sorted, "2B")
pr_3B <- pos_pool(h_sorted, "3B")
pr_SS <- pos_pool(h_sorted, "SS")
pr_OF <- pos_pool(h_sorted, "OF")
pr_UT <- pos_pool(h_sorted, "UT")

get_pr <- function(nkey, pos_str) {
  if (is.na(pos_str) || !nzchar(pos_str)) return(NA_character_)
  primary <- trimws(strsplit(pos_str, ",")[[1]][1])
  df <- switch(primary,
    "C"  = pr_C,  "1B" = pr_1B, "2B" = pr_2B, "3B" = pr_3B,
    "SS" = pr_SS, "OF" = pr_OF, "UT" = pr_UT,  NULL)
  if (is.null(df)) return(NA_character_)
  m <- df[df$name_key == nkey, ]
  if (nrow(m) == 0) return(NA_character_)
  paste0(primary, m$pr[1])
}

# ── LOAD FANTRAX + MATCH ──────────────────────────────────────────────────────
fantrax <- read_csv(FANTRAX_PATH, show_col_types = FALSE) %>%
  mutate(name_key = norm_name(Player)) %>%
  filter(as.numeric(RkOv) <= RKOVER_MAX)          # drops name-collision phantoms

wire <- fantrax %>%
  left_join(
    h_sorted %>% select(name_key, positions, pa, r, hr, rbi, sb, obp,
                        dollar_value, overall_rank),
    by = "name_key"
  ) %>%
  filter(!is.na(dollar_value), dollar_value >= MIN_DV) %>%
  arrange(desc(dollar_value)) %>%
  head(TOP_N)

wire$pos_rank <- mapply(get_pr, wire$name_key, wire$positions)

# ── HITTER STREAMONATOR ───────────────────────────────────────────────────────
cat("Fetching SP Skillz + probables + TR + PF for hitter streamonator...\n")
spz_std <- spz_gen_compute(
  spz_gen_parse(spz_gen_fetch(spz_gen_build_url(2026, 0))), ip_min = 15)
spz_l30 <- spz_gen_compute(
  spz_gen_parse(spz_gen_fetch(spz_gen_build_url(2026, 3))), ip_min = 5)
prob    <- stream_parse_probables(stream_fg_fetch(STREAM_PROBABLES_URL))$data
tr_std  <- trater_score(trater_parse(trater_fg_fetch(trater_build_url(2026, 0))))
tr_l30  <- trater_score(trater_parse(trater_fg_fetch(trater_build_url(2026, 3))))
pf      <- stream_load_pf()

stream_h <- hit_stream_build(
  probables  = prob,
  spz_std    = spz_std, spz_l30 = spz_l30, pf = pf,
  tr_data    = list(std = tr_std, l30 = tr_l30),
  week_start = WEEK_START, week_end = WEEK_END,
  w_g = 2, w_pitcher = 2, w_park = 1, w_team = 0.5
)

stream_lookup <- function(team_abbr) {
  if (is.null(stream_h) || nrow(stream_h) == 0) return(NA_real_)
  m <- stream_h[stream_norm_team(stream_h$team) == stream_norm_team(team_abbr), ]
  if (nrow(m) == 0) return(NA_real_)
  round(as.numeric(m$score[1]), 1)
}

wire$stream_score <- sapply(wire$Team, stream_lookup)

# ── OUTPUT ────────────────────────────────────────────────────────────────────
cat(sprintf("\n=== STEP 2: WIRE HITTERS — RoS Value >= $%.0f (top %d, Fantrax FA, RkOv <= %d) ===\n",
    MIN_DV, TOP_N, RKOVER_MAX))
cat(sprintf("Week: %s to %s\n\n", WEEK_START, WEEK_END))

cat(sprintf("  %-22s %-4s %-12s %4s %6s %5s %5s %5s %5s %5s %7s %6s\n",
    "Player", "Team", "Pos", "OvRk", "PosRnk",
    "RoS_R", "RoS_HR", "RoS_RBI", "RoS_SB", "RoS_OBP", "$Val", "Stream"))
cat("  ", strrep("-", 102), "\n", sep = "")

for (i in seq_len(nrow(wire))) {
  p <- wire[i, ]
  pos_str <- if (!is.na(p$positions)) p$positions else p$Position
  cat(sprintf("  %-22s %-4s %-12s %4d %6s %5.0f %5.0f %5.0f %5.0f %5.3f %7s %6s\n",
    p$Player, p$Team,
    substr(pos_str, 1, 12),
    p$overall_rank,
    ifelse(is.na(p$pos_rank), "?", p$pos_rank),
    p$r, p$hr, p$rbi, p$sb, p$obp,
    sprintf("$%.0f", p$dollar_value),
    ifelse(is.na(p$stream_score), "—", p$stream_score)))
}

cat("\n--- RoS $Val: OBP-scored, CK Weights, ATC RoS PT (same settings as Step 1)\n")
cat("--- Stream: team batting environment score for the week (100=avg; higher = better matchups)\n")
cat("--- Primary use: RoS $val. Streamonator is secondary (short-term context only)\n")
cat(sprintf("\nSource: %s\n", basename(XLSX_PATH)))
