# ── Step 5: Wire Closer / Save Source Analysis ────────────────────────────────
# PURPOSE: Identify wire RPs with saves potential, tiered by role + skills.
#
# PRIORITY ORDER:
#   TIER 1  — Labeled closer (FG/CM) + great RP Skillz (>= 100)
#   TIER 2  — Labeled sole closer + poor RP Skillz (<100 or no data)
#   TIER 3  — Committee closer + great RP Skillz (>= 100)   [≈ equal to Tier 2]
#   TIER 4  — 1st/2nd in line + high RPZ (speculative upside)
#   TIER 5  — High RPZ, not in save picture
#   TIER 6  — Low RPZ, low leverage (generally skip)
#
# INPUTS (update each week):
#   FANTRAX_PATH — Fantrax pitcher export CSV
#   closer_roles — CloserMonkey data (hardcoded from screenshot, update weekly)

suppressPackageStartupMessages({
  library(dplyr); library(readr); library(jsonlite); library(curl); library(httr)
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
source(file.path(base, "R/rp_skillz.R"))
source(file.path(base, "R/modules/mod_rp_skillz.R"))

norm_name <- function(x) trimws(gsub("[^a-z ]", "", tolower(iconv(x, to="ASCII//TRANSLIT"))))

# ── CONFIG ────────────────────────────────────────────────────────────────────
FANTRAX_PATH  <- "/Users/ckaufman/Downloads/Fantrax-Players-A Slog to Rigor Mortis(3).csv"
if (!file.exists(FANTRAX_PATH)) stop(paste0(
  "File not found: ", FANTRAX_PATH, "\n",
  "  Update FANTRAX_PATH above with the current filename.\n",
  "  If you switched machines (Mac ↔ Windows), also update the path prefix:\n",
  "    Mac:     /Users/ckaufman/Downloads/<filename>\n",
  "    Windows: C:/Users/Collin/Downloads/<filename>"
))
RKOVER_MAX    <- 750L
RPZ_HIGH_THR  <- 100     # "great" RPZ threshold for tier breakpoints

# ── CLOSER ROLE LOOKUP — FG Excel 4/26/26 + CloserMonkey 4/26/26 ─────────────
# Primary source: FG Excel export. CM used where FG conflicts or is ambiguous.
# role: CLOSER | COMMITTEE | 1ST_LINE | 2ND_LINE
# sole = TRUE means sole closer (not committee). Join on name_key + team.
closer_roles <- rbind(
  # AL East — FG
  data.frame(name="ryan helsley",         team="BAL", role="CLOSER",    sole=TRUE),
  data.frame(name="rico garcia",           team="BAL", role="1ST_LINE",  sole=TRUE),
  data.frame(name="anthony nunez",         team="BAL", role="2ND_LINE",  sole=TRUE),
  data.frame(name="aroldis chapman",       team="BOS", role="CLOSER",    sole=TRUE),
  data.frame(name="garrett whitlock",      team="BOS", role="1ST_LINE",  sole=TRUE),
  data.frame(name="greg weissert",         team="BOS", role="2ND_LINE",  sole=TRUE),
  data.frame(name="david bednar",          team="NYY", role="CLOSER",    sole=TRUE),
  data.frame(name="fernando cruz",         team="NYY", role="1ST_LINE",  sole=TRUE),
  data.frame(name="brent headrick",        team="NYY", role="2ND_LINE",  sole=TRUE),
  data.frame(name="bryan baker",           team="TB",  role="CLOSER",    sole=TRUE),
  data.frame(name="griffin jax",           team="TB",  role="1ST_LINE",  sole=TRUE),
  data.frame(name="ian seymour",           team="TB",  role="2ND_LINE",  sole=TRUE),
  data.frame(name="louis varland",         team="TOR", role="CLOSER",    sole=TRUE),
  data.frame(name="tyler rogers",          team="TOR", role="1ST_LINE",  sole=TRUE),
  data.frame(name="jeff hoffman",          team="TOR", role="2ND_LINE",  sole=TRUE),
  # AL Central — FG
  data.frame(name="seranthony dominguez",  team="CHW", role="CLOSER",    sole=TRUE),
  data.frame(name="grant taylor",          team="CHW", role="1ST_LINE",  sole=TRUE),
  data.frame(name="sean newcomb",          team="CHW", role="2ND_LINE",  sole=TRUE),
  data.frame(name="cade smith",            team="CLE", role="CLOSER",    sole=TRUE),
  data.frame(name="hunter gaddis",         team="CLE", role="1ST_LINE",  sole=TRUE),
  data.frame(name="erik sabrowski",        team="CLE", role="2ND_LINE",  sole=TRUE),
  data.frame(name="kenley jansen",         team="DET", role="CLOSER",    sole=TRUE),
  data.frame(name="kyle finnegan",         team="DET", role="1ST_LINE",  sole=TRUE),
  data.frame(name="will vest",             team="DET", role="2ND_LINE",  sole=TRUE),
  data.frame(name="lucas erceg",           team="KC",  role="CLOSER",    sole=TRUE),
  data.frame(name="matt strahm",           team="KC",  role="1ST_LINE",  sole=TRUE),
  data.frame(name="nick mears",            team="KC",  role="2ND_LINE",  sole=TRUE),
  data.frame(name="cole sands",            team="MIN", role="COMMITTEE", sole=FALSE),
  data.frame(name="justin topa",           team="MIN", role="COMMITTEE", sole=FALSE),
  data.frame(name="eric orze",             team="MIN", role="COMMITTEE", sole=FALSE),
  # AL West — FG
  data.frame(name="jack perkins",          team="ATH", role="COMMITTEE", sole=FALSE),
  data.frame(name="hogan harris",          team="ATH", role="COMMITTEE", sole=FALSE),
  data.frame(name="joel kuhnel",           team="ATH", role="COMMITTEE", sole=FALSE),
  data.frame(name="enyel de los santos",   team="HOU", role="CLOSER",    sole=TRUE),
  data.frame(name="bryan king",            team="HOU", role="1ST_LINE",  sole=TRUE),
  data.frame(name="bryan abreu",           team="HOU", role="2ND_LINE",  sole=TRUE),
  data.frame(name="sam bachman",           team="LAA", role="COMMITTEE", sole=FALSE),
  data.frame(name="chase silseth",         team="LAA", role="COMMITTEE", sole=FALSE),
  data.frame(name="drew pomeranz",         team="LAA", role="COMMITTEE", sole=FALSE),
  data.frame(name="andres munoz",          team="SEA", role="CLOSER",    sole=TRUE),
  data.frame(name="matt brash",            team="SEA", role="1ST_LINE",  sole=TRUE),
  data.frame(name="eduard bazardo",        team="SEA", role="2ND_LINE",  sole=TRUE),
  data.frame(name="jake latz",             team="TEX", role="CLOSER",    sole=TRUE),  # CM: Latz; FG shows Cole Winn as setup
  data.frame(name="cole winn",             team="TEX", role="1ST_LINE",  sole=TRUE),
  # NL East — FG
  data.frame(name="brad keller",           team="PHI", role="CLOSER",    sole=TRUE),
  data.frame(name="jose alvarado",         team="PHI", role="1ST_LINE",  sole=TRUE),
  data.frame(name="orion kerkering",       team="PHI", role="2ND_LINE",  sole=TRUE),
  data.frame(name="devin williams",        team="NYM", role="CLOSER",    sole=TRUE),
  data.frame(name="luke weaver",           team="NYM", role="1ST_LINE",  sole=TRUE),
  data.frame(name="brooks raley",          team="NYM", role="2ND_LINE",  sole=TRUE),
  data.frame(name="pete fairbanks",        team="MIA", role="CLOSER",    sole=TRUE),
  data.frame(name="tyler phillips",        team="MIA", role="1ST_LINE",  sole=TRUE),
  data.frame(name="john king",             team="MIA", role="2ND_LINE",  sole=TRUE),
  data.frame(name="gus varland",           team="WSH", role="COMMITTEE", sole=FALSE),
  data.frame(name="pj poulin",             team="WSH", role="COMMITTEE", sole=FALSE),
  data.frame(name="brad lord",             team="WSH", role="COMMITTEE", sole=FALSE),
  # NL Central — FG
  data.frame(name="ben brown",             team="CHC", role="COMMITTEE", sole=FALSE),
  data.frame(name="jacob webb",            team="CHC", role="COMMITTEE", sole=FALSE),
  data.frame(name="hoby milner",           team="CHC", role="COMMITTEE", sole=FALSE),
  data.frame(name="emilio pagan",          team="CIN", role="CLOSER",    sole=TRUE),
  data.frame(name="tony santillan",        team="CIN", role="1ST_LINE",  sole=TRUE),
  data.frame(name="graham ashcraft",       team="CIN", role="2ND_LINE",  sole=TRUE),
  data.frame(name="abner uribe",           team="MIL", role="CLOSER",    sole=TRUE),
  data.frame(name="trevor megill",         team="MIL", role="1ST_LINE",  sole=TRUE),
  data.frame(name="aaron ashby",           team="MIL", role="2ND_LINE",  sole=TRUE),  # FG: Ashby 2nd — KEY
  data.frame(name="riley obrien",          team="STL", role="CLOSER",    sole=TRUE),
  data.frame(name="jojo romero",           team="STL", role="1ST_LINE",  sole=TRUE),
  data.frame(name="ryne stanek",           team="STL", role="2ND_LINE",  sole=TRUE),
  data.frame(name="isaac mattson",         team="PIT", role="1ST_LINE",  sole=TRUE),  # FG: no clear closer for PIT
  data.frame(name="gregory soto",          team="PIT", role="2ND_LINE",  sole=TRUE),
  # NL West — FG
  data.frame(name="victor vodnik",          team="COL", role="CLOSER",    sole=FALSE),
  data.frame(name="paul sewald",           team="ARI", role="CLOSER",    sole=TRUE),
  data.frame(name="juan morillo",          team="ARI", role="1ST_LINE",  sole=TRUE),
  data.frame(name="jonathan loaisiga",     team="ARI", role="2ND_LINE",  sole=TRUE),
  data.frame(name="tanner scott",          team="LAD", role="COMMITTEE", sole=FALSE),  # FG: LAD committee
  data.frame(name="alex vesia",            team="LAD", role="COMMITTEE", sole=FALSE),
  data.frame(name="blake treinen",         team="LAD", role="COMMITTEE", sole=FALSE),
  data.frame(name="mason miller",          team="SD",  role="CLOSER",    sole=TRUE),
  data.frame(name="jason adam",            team="SD",  role="1ST_LINE",  sole=TRUE),
  data.frame(name="adrian morejon",        team="SD",  role="2ND_LINE",  sole=TRUE),
  data.frame(name="ryan walker",           team="SF",  role="CLOSER",    sole=TRUE),
  data.frame(name="erik miller",           team="SF",  role="1ST_LINE",  sole=TRUE),
  data.frame(name="keaton winn",           team="SF",  role="2ND_LINE",  sole=TRUE),
  data.frame(name="robert suarez",         team="ATL", role="CLOSER",    sole=TRUE),
  data.frame(name="dylan lee",             team="ATL", role="1ST_LINE",  sole=TRUE),  # FG: Lee 1st, Kinley 2nd
  data.frame(name="tyler kinley",          team="ATL", role="2ND_LINE",  sole=TRUE),
  stringsAsFactors = FALSE
)
closer_roles$name_key <- norm_name(closer_roles$name)

# ── LOAD FANTRAX ──────────────────────────────────────────────────────────────
fantrax <- read_csv(FANTRAX_PATH, show_col_types = FALSE) %>%
  mutate(
    name_key = norm_name(Player),
    RkOv     = suppressWarnings(as.integer(RkOv)),
    IP       = suppressWarnings(as.numeric(IP)),
    ERA      = suppressWarnings(as.numeric(ERA)),
    WHIP     = suppressWarnings(as.numeric(WHIP)),
    SV       = suppressWarnings(as.integer(SV)),
    GP       = suppressWarnings(as.integer(GP))
  ) %>%
  filter(
    Position == "RP",
    Status   == "FA"
  ) %>%
  arrange(RkOv)

# ── FETCH RP SKILLZ ───────────────────────────────────────────────────────────
cat("Fetching RP Skillz (STD + L30)...\n")
rpz_std_fmt <- rpz_gen_format(rpz_gen_compute(rpz_gen_parse(rpz_gen_fetch(rpz_gen_build_url(2026, 0)))))
rpz_l30_fmt <- rpz_gen_format(rpz_gen_compute(rpz_gen_parse(rpz_gen_fetch(rpz_gen_build_url(2026, 3)))))
rpz_std_fmt$name_key <- norm_name(rpz_std_fmt$Player)
rpz_l30_fmt$name_key <- norm_name(rpz_l30_fmt$Player)

# ── MERGE ─────────────────────────────────────────────────────────────────────
wire <- fantrax %>%
  left_join(
    rpz_std_fmt %>% select(name_key, rpz_std = Score, stuff_std = `Stuff+`,
                           kpct_std = `K%`, gmli_std = gmLI, sdmd_std = `SD-MD`),
    by = "name_key"
  ) %>%
  left_join(
    rpz_l30_fmt %>% select(name_key, rpz_l30 = Score, gmli_l30 = gmLI, sdmd_l30 = `SD-MD`),
    by = "name_key"
  ) %>%
  left_join(
    closer_roles %>% select(name_key, team = team, cm_role = role, cm_sole = sole),
    by = c("name_key", "Team" = "team")
  ) %>%
  mutate(
    rpz_primary  = coalesce(rpz_l30, rpz_std),
    gmli_primary = coalesce(gmli_l30, gmli_std),
    sdmd_primary = coalesce(sdmd_l30, sdmd_std),
    cm_role      = replace(cm_role, is.na(cm_role), "—"),
    cm_sole      = replace(cm_sole, is.na(cm_sole), FALSE),
    tier = case_when(
      cm_role == "CLOSER" & !is.na(rpz_primary) & rpz_primary >= RPZ_HIGH_THR           ~ 1L,
      cm_role == "CLOSER"                                                                ~ 2L,
      cm_role == "COMMITTEE" & !is.na(rpz_primary) & rpz_primary >= RPZ_HIGH_THR        ~ 3L,
      (cm_role %in% c("1ST_LINE","2ND_LINE")) & !is.na(rpz_primary) & rpz_primary >= RPZ_HIGH_THR ~ 4L,
      !is.na(rpz_primary) & rpz_primary >= RPZ_HIGH_THR                                 ~ 5L,
      TRUE                                                                               ~ 6L
    ),
    tier_label = c("1"="CLOSER + HIGH RPZ",
                   "2"="CLOSER, LOW/NO RPZ",
                   "3"="COMMITTEE + HIGH RPZ",
                   "4"="IN LINE + HIGH RPZ (speculative)",
                   "5"="HIGH RPZ, NO SAVE ROLE",
                   "6"="LOW RPZ / LOW LI")[as.character(tier)]
  ) %>%
  arrange(tier, desc(rpz_primary), desc(gmli_primary))

# ── OUTPUT ────────────────────────────────────────────────────────────────────
cat(sprintf("\n=== STEP 5: WIRE CLOSERS / SAVE SOURCES ===\n"))
cat(sprintf("Source: CloserMonkey 4/26/26 + FG depth chart + RP Skillz\n"))
cat(sprintf("RPZ >= %d = 'high'; pool avg = 100\n\n", RPZ_HIGH_THR))

tier_order <- sort(unique(wire$tier))

for (t in tier_order) {
  sub <- wire[wire$tier == t, ]
  if (nrow(sub) == 0) next
  lbl <- sub$tier_label[1]
  cat(sprintf("── TIER %d: %s ──────────────────────────────────────────────\n", t, lbl))
  cat(sprintf("  %-22s %-4s %4s %5s %4s | %6s %6s %6s %5s %5s %5s | %8s\n",
      "Player", "Team", "RkOv", "IP", "SV",
      "RPZ_S", "RPZ_L", "Stf+", "K%", "gmLI", "SD-MD",
      "CM Role"))
  cat("  ", strrep("-", 106), "\n", sep = "")
  for (i in seq_len(nrow(sub))) {
    p <- sub[i, ]
    cat(sprintf("  %-22s %-4s %4d %5s %4s | %6s %6s %6s %5s %5s %5s | %8s\n",
      p$Player, p$Team,
      p$RkOv,
      ifelse(is.na(p$IP),  "—", sprintf("%.1f", p$IP)),
      ifelse(is.na(p$SV),  "—", as.character(p$SV)),
      ifelse(is.na(p$rpz_std),     "—", round(p$rpz_std, 0)),
      ifelse(is.na(p$rpz_l30),     "—", round(p$rpz_l30, 0)),
      ifelse(is.na(p$stuff_std),   "—", as.character(p$stuff_std)),
      ifelse(is.na(p$kpct_std),    "—", sprintf("%.1f", p$kpct_std)),
      ifelse(is.na(p$gmli_primary),"—", sprintf("%.2f", p$gmli_primary)),
      ifelse(is.na(p$sdmd_primary),"—", as.character(p$sdmd_primary)),
      p$cm_role))
  }
  cat("\n")
}

cat("── SCALE ──────────────────────────────────────────────────────────────────\n")
cat("  RPZ Score: 100=pool avg | >110=elite | gmLI: 1.0=avg, closers 1.5-2.0+\n")
cat("  Tier 1 & 2 equal priority per user; Tier 3 ≈ Tier 2 (committee+skills)\n")
cat("  CloserMonkey source: 4/26/26 — update closer_roles block each week\n")
