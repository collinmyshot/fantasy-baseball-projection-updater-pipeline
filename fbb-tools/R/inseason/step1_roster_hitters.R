# ── Step 1: Roster Hitter Analysis ────────────────────────────────────────────
# PURPOSE: Given a roster (hardcoded below) and an app auction value export,
#   produce tiered RoS projection table with dollar values, overall rank,
#   positional rank, and PT-boost overrides.
#
# INPUTS (update each week):
#   1. ROSTER BLOCK — hardcode player names, rank_pos, boosted_dv, boost_note
#   2. XLSX_PATH    — most recent app export (OBP-scored, with positions column)
#
# REQUIREMENTS:
#   - App export must have: playerid, name, team, positions, pa, r, hr, rbi,
#     sb, obp, dollar_value columns (OBP scoring, CK Weights, ATC RoS PT)
#   - PT boosts come from the app's Compare tab hypothetical tool; plug in
#     the dollar value at your chosen PA target and note the PA in boost_note
#
# KEY SETTINGS CONFIRMED:
#   League:  11-team | $260 | 70/30 hit/pitch | 5x5 OBP roto
#   Weights: CK (HR=1.35, R=RBI=0.6, SB=1, OBP=1, PA vol=2.5)
#   PT src:  ATC (RoS) | Min PA: 100
#   Proj:    Steamer×2, ATC×1, BATX×3, OOPSY×3 (ZiPS/THE BAT = 0)

suppressPackageStartupMessages({
  library(dplyr); library(openxlsx)
})

norm_name <- function(x) tolower(trimws(gsub("[^a-z ]", "", iconv(x, to="ASCII//TRANSLIT"))))

# ── CONFIG — update each swap ─────────────────────────────────────────────────

XLSX_PATH <- "/Users/ckaufman/Downloads/auction_values_roto_2026-04-26(5).xlsx"

# ── ROSTER — update names/boosts each swap ────────────────────────────────────
# rank_pos:   primary position used for positional rank (C/1B/2B/3B/SS/OF/UT)
# boosted_dv: dollar value from app Compare tab at boosted PA; NA = use export
# boost_note: label shown in $Val column, e.g. "(500PA*)"; "" = no boost

roster <- data.frame(
  name       = c(
    "Aaron Judge", "Shohei Ohtani", "Yordan Alvarez", "James Wood",
    "Pete Alonso", "Miguel Vargas", "Bryan Reynolds", "Max Muncy",
    "Xander Bogaerts", "Carlos Correa", "Ezequiel Tovar",
    "Salvador Perez", "Masyn Winn", "Heliot Ramos",
    "Jesus Sanchez", "Edouard Julien", "Nasim Nunez", "Leody Taveras"
  ),
  rank_pos   = c(
    "OF", "UT", "UT", "OF",
    "1B", "3B", "OF", "3B",
    "SS", "SS", "SS",
    "C",  "SS", "OF",
    "OF", "2B", "2B", "OF"
  ),
  boosted_dv = c(
    NA,    NA,    NA,    NA,
    NA,    NA,    NA,    13.70,
    NA,    NA,    NA,
    NA,    NA,    NA,
    NA,    NA,    2.61,  4.95
  ),
  boost_note = c(
    "", "", "", "",
    "", "", "", "(500PA*)",
    "", "", "",
    "", "", "",
    "", "", "(500PA*)", "(450PA*)"
  ),
  stringsAsFactors = FALSE
)
roster$name_key <- norm_name(roster$name)

# ── TIERS — adjust groupings per roster ──────────────────────────────────────
tiers <- list(
  list(label = "TIER 1 — Elite anchors",
       names = c("Aaron Judge", "Shohei Ohtani", "Yordan Alvarez", "James Wood")),
  list(label = "TIER 2 — Solid positive value",
       names = c("Pete Alonso", "Miguel Vargas", "Bryan Reynolds",
                 "Max Muncy", "Xander Bogaerts")),
  list(label = "TIER 3 — Around replacement / rostered for position",
       names = c("Carlos Correa", "Ezequiel Tovar", "Salvador Perez",
                 "Masyn Winn", "Heliot Ramos")),
  list(label = "TIER 4 — Reserve / fringe",
       names = c("Jesus Sanchez", "Edouard Julien", "Nasim Nunez", "Leody Taveras"))
)

# ── LOAD + RANK ───────────────────────────────────────────────────────────────
h <- read.xlsx(XLSX_PATH, sheet = "Hitters")
h$playerid <- as.character(h$playerid)
merged <- h %>% arrange(desc(dollar_value))
merged$name_key    <- norm_name(merged$name)
merged$overall_rank <- seq_len(nrow(merged))

pos_pool <- function(df, pattern) {
  sub <- df[!is.na(df$positions) & grepl(pattern, df$positions, ignore.case = TRUE), ]
  sub <- sub[order(-sub$dollar_value), ]
  data.frame(name_key = sub$name_key, pr = seq_len(nrow(sub)), stringsAsFactors = FALSE)
}
pr_C  <- pos_pool(merged, "\\bC\\b")
pr_1B <- pos_pool(merged, "1B")
pr_2B <- pos_pool(merged, "2B")
pr_3B <- pos_pool(merged, "3B")
pr_SS <- pos_pool(merged, "SS")
pr_OF <- pos_pool(merged, "OF")
pr_UT <- pos_pool(merged, "UT")

get_pr <- function(nkey, ppos) {
  df <- switch(ppos,
    "C"  = pr_C,  "1B" = pr_1B, "2B" = pr_2B, "3B" = pr_3B,
    "SS" = pr_SS, "OF" = pr_OF, "UT" = pr_UT,  NULL)
  if (is.null(df)) return(NA_character_)
  m <- df[df$name_key == nkey, ]
  if (nrow(m) == 0) return(NA_character_)
  paste0(ppos, m$pr[1])
}

# ── PRINT ─────────────────────────────────────────────────────────────────────
hdr <- function() {
  cat(sprintf("  %-22s %-9s %4s %6s %5s %5s %5s %5s %5s %8s\n",
    "Player", "Pos", "OvRk", "PosRnk", "RoS_R", "RoS_HR", "RoS_RBI",
    "RoS_SB", "RoS_OBP", "$Val"))
  cat("  ", strrep("-", 88), "\n", sep = "")
}

for (tier in tiers) {
  cat("\n", tier$label, "\n", sep = "")
  hdr()
  for (nm in tier$names) {
    nkey <- norm_name(nm)
    p    <- roster[roster$name_key == nkey, ]
    row  <- merged[merged$name_key == nkey, ]
    if (nrow(row) == 0 || nrow(p) == 0) { cat("  NOT FOUND:", nm, "\n"); next }
    row <- row[1, ]; p <- p[1, ]

    dv   <- if (!is.na(p$boosted_dv)) p$boosted_dv else row$dollar_value
    note <- p$boost_note
    pr   <- get_pr(nkey, p$rank_pos)
    pos  <- if (!is.na(row$positions)) row$positions else "?"

    cat(sprintf("  %-22s %-9s %4d %6s %5.0f %5.0f %5.0f %5.0f %5.3f %8s %s\n",
      p$name, pos, row$overall_rank,
      ifelse(is.na(pr), "?", pr),
      row$r, row$hr, row$rbi, row$sb, row$obp,
      sprintf("$%.0f", dv), note))
  }
}
cat("\n* PA-boosted from app Compare tab (overall rank at default projected PA)\n")
cat(sprintf("\nSource: %s\n", basename(XLSX_PATH)))
